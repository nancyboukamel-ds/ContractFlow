"""
This is the parent orchestrator workflow that builds on the PDFSummaryWorkflow from before.
It's significantly more sophisticated: it demonstrates fan-out/fan-in parallelism with child workflows, 
human-in-the-loop (HITL) approval cycles, and Temporal's query/signal/update primitives for external interaction
with a running workflow. Let me go through it section by section.
"""

import asyncio
from dataclasses import dataclass
from datetime import timedelta
from typing import Optional
import json

from temporalio import workflow
from temporalio.common import RetryPolicy
##ApplicationError: Temporal's mechanism for deliberately failing a workflow with a custom, catchable error 
from temporalio.exceptions import ApplicationError
## ParentClosePolicy: governs what happens to child workflows if this parent workflow completes, fails, or is terminated 
from temporalio.workflow import ParentClosePolicy

from prompts import _SYNTHESIS_PROMPT, _REVISION_PROMPT

with workflow.unsafe.imports_passed_through():
    from activities import (
        call_llm, CallLLMInput,
    )
    from child_workflow import (
        PDFSummaryWorkflow, PDFSummaryInput
    )

# ── Input / Output ────────────────────────────────────────────────────────────

@dataclass
class ContractReviewInput:
    s3_paths: list
    ## caps how many times a human reviewer can send the report back for AI revision before the workflow gives up and just returns as-is.
    max_revisions: int = 2

@dataclass
class ContractReviewOutput:
    ## Output bundles the final synthesized report, the list of source documents (sources), and who approved it (approved_by).
    report: str
    sources: list
    approved_by: str

DEFAULT_RETRY_POLICY = RetryPolicy(
    initial_interval=timedelta(seconds=3),
    backoff_coefficient=2.0,
    maximum_interval=timedelta(seconds=60),
    maximum_attempts=4,
)

@workflow.defn
class ContractReviewWorkflow:

    ## this workflow is stateful and long-lived — it needs to hold state across a potentially multi-day human review cycle, and expose that state to external callers via queries.
    def __init__(self):
        self._status: str = "processing"
        self._summaries: list = []
        self._report: dict = {}
        self._llm_attempts: dict = {} 

        self._review_decision: Optional[str] = None
        self._review_feedback: str = ""
        self._approved_by: str = ""


    # ── Query: status ───────────────────────────────
    ## Queries let an external client synchronously ask a running (or even completed) workflow for its current in-memory state, without affecting workflow execution. 

    @workflow.query
    ## gives a lightweight summary (useful for a dashboard/polling UI) status string, count of PDFs processed, a truncated 500-char preview of the report, and who approved it.
    def get_status(self) -> dict:

        return {
            "status":         self._status,
            "pdfs_processed": len(self._summaries),
            "report_preview": json.dumps(self._report, ensure_ascii=False)[:500],
            "approved_by":    self._approved_by,
        }
    
    # ── Query: full report — call this before submitting a review decision ─────
    @workflow.query
    def get_report(self) -> dict:
        return {
            "status":      self._status,
            "report":      self._report,
            "approved_by": self._approved_by,
            "sources":     [s["s3_path"] for s in self._summaries],
        }

    # ── Signal: record who is reviewing ──────────────────────────────────────
    # Signals are asynchronous, one-way messages sent into a running workflow from the outside they can mutate state (unlike queries), but they don't return a value to the caller (fire-and-forget). 
    # , a reviewer logging into a UI) calls this to record who is currently reviewing the contract, before making a decision.
    @workflow.signal
    async def assign_reviewer(self, name: str) -> None:
        self._approved_by = name

    ## Updates are the newer, more powerful sibling of signals: like signals, they can mutate workflow state, but like a normal function call, they synchronously return a value to the caller and can be validated before being accepted into workflow history.
    @workflow.update
    async def submit_decision(self, decision: str, feedback: str = "") -> str:
        self._review_decision = decision
        self._review_feedback = feedback

        return f"Decision '{decision}' recorded."
    
    ## runs before the update is admitted
    @submit_decision.validator
    def validate_decision(self, decision: str, feedback: str = "") -> None:
        if decision not in ("approve", "revise"):
            raise ValueError(f"Must be 'approve' or 'revise', got: '{decision}'")
        
        if decision == "revise" and not feedback.strip():
            raise ValueError("Feedback is required when requesting a revision.")



    @workflow.run
    async def run(self, params: ContractReviewInput) -> ContractReviewOutput:
        
        # Step 1: Fan-out — one child per PDF, all in parallel
        ##  it starts one PDFSummaryWorkflow child workflow per PDF, all concurrently:

        self._status = "extracting"

        workflow.logger.info(f"Fanning out to {len(params.s3_paths)} child workflows")

        workflow_id = workflow.info().workflow_id
        workflow_task_queue = workflow.info().task_queue

        # TERMINATE: kill child workflows when parent closes
        # REQUEST_CANCEL: ask child workflows to cancel gracefully
        # ABANDON: leave them alone and let them keep running

        ##  returns a handle as soon as the child is started  (asyncio.gather)
        ##  to kick off all children essentially simultaneously rather than sequentially awaiting each one's start.
        handles = await asyncio.gather(
            *[
               workflow.start_child_workflow(
                   PDFSummaryWorkflow.run ,
                   PDFSummaryInput(
                       s3_path=current_s3_path
                   ),
                   ## Each child gets a deterministic, human-readable workflow ID
                   ## useful for debugging/observability 
                   id=f"{workflow_id}-pdf-{idx+1}",
                   ## reuses the same task queue as the parent, so the same worker pool picks up child workflow tasks.
                   task_queue=workflow_task_queue,
                   # ABANDON: leave children running independently, detached from parent's lifecycle
                   # TERMINATE: forcibly kill children if parent closes
                   # REQUEST_CANCEL: ask children to cancel gracefully
                   parent_close_policy=ParentClosePolicy.ABANDON
               )

               for idx, current_s3_path in enumerate(params.s3_paths)
             ]
        )

        ## Step 2: Fan-In  Await All Children, Tolerate Partial Failure
        ## fan-in step, awaiting each child handle's actual completion
        raw_results = await asyncio.gather(
            *handles,
            return_exceptions=True,
        )

        for i, res in enumerate(raw_results):

            if isinstance(res, Exception):
                workflow.logger.warning(f"PDF {i} failed: {res}")
            else:
                self._summaries.append({
                    "s3_path":   res.s3_path,
                    "summary":   res.summary,
                    "key_risks": res.key_risks,
                })

        ## if literally all PDFs failed, the workflow gives up entirely via ApplicationError("All PDFs failed to process.")
        ## But if even one PDF succeeded, the workflow proceeds with whatever it has. This is a reasonable partial-failure tolerance strategy for a multi-document batch job.
        if len(self._summaries) == 0:
            raise ApplicationError("All PDFs failed to process.")
        
        # Step 3: Synthesize all summaries into a risk report
        self._status = "analyzing"
        workflow.logger.info(f"Synthesizing {len(self._summaries)} summaries")

        combined_summary = "\n\n".join([

            f"**Contract {i+1}** (`{summary['s3_path']}`):\n"
            f"Summary: {summary['summary']}\n"
            f"Risks: {summary['key_risks']}"

            for i, summary in enumerate(self._summaries)
        ])

        llm_prompt = _SYNTHESIS_PROMPT.format(
            summaries=combined_summary,
            n=len(self._summaries)
        )

        llm_result = await workflow.execute_activity(
            call_llm,
            CallLLMInput(
                prompt=llm_prompt,
                schema_name="SynthesisReportSchema"
            ),
            start_to_close_timeout=timedelta(minutes=3),
            heartbeat_timeout=timedelta(seconds=180),
            retry_policy=DEFAULT_RETRY_POLICY,
        )

        self._report = llm_result.data
        self._llm_attempts["synthesis"] = llm_result.attempts
        # Step 3: HITL — pause until a human approves or requests revision.
        # The reviewer should call get_report (Query) to read the full report
        # before calling submit_decision (Update).

        for revision_no in range(params.max_revisions + 1):

            self._status = "awaiting-review"
            workflow.logger.info(f"Waiting for human review (cycle {revision_no})")

            self._review_decision = None

            try:
                ## this is Temporal's mechanism for a workflow to pause and sleep, deterministically, until some in-memory condition becomes true
                ## the workflow is literally suspended in Temporal's persisted state, consuming no worker resources, and wakes up only when a relevant event (the update) arrives or the timeout fires.
                await workflow.wait_condition(
                    lambda: self._review_decision is not None,
                    timeout=timedelta(days=3),
                )
            except asyncio.TimeoutError:
                workflow.logger.warning("Review timed out after 3 days — auto-completing")
                break

            if self._review_decision == "approve":
                workflow.logger.info(f"Approved by: {self._approved_by}")
                break

            self._status = "revising"
            workflow.logger.info(f"Revising — feedback: {self._review_feedback}")

            ##  feeding it the current report (serialized as pretty-printed JSON) plus the reviewer's free-text feedback, presumably asking it to produce a revised report addressing that feedback. 
            llm_prompt = _REVISION_PROMPT.format(
                report=json.dumps(
                    self._report, ensure_ascii=False, indent=2
                ),

                feedback=self._review_feedback,
            )

            ## If max_revisions is exhausted without an "approve", the for loop simply ends naturally (not via break), and the workflow proceeds with whatever the last revised report was, having never been explicitly approved (self._approved_by might remain empty in that case unless assign_reviewer was called separately).
            revised_report = await workflow.execute_activity(
                call_llm,
                CallLLMInput(prompt=llm_prompt,schema_name="SynthesisReportSchema"),
                start_to_close_timeout=timedelta(minutes=3),
                heartbeat_timeout=timedelta(seconds=180),
                retry_policy=DEFAULT_RETRY_POLICY,
            )

            self._report = revised_report.data
            self._llm_attempts[f"revision_{revision_no}"] = revised_report.attempts

        # REVISED COMPLETED
        self._status = "completed"
        return ContractReviewOutput(
            report=self._report,
            sources=[s["s3_path"] for s in self._summaries],
            approved_by=self._approved_by,
        )



            