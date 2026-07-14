from dataclasses import dataclass
from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy

## _SUMMARY_PROMPT is presumably a string template imported from a prompts module, containing a {text} placeholder.
from prompts import _SUMMARY_PROMPT


with workflow.unsafe.imports_passed_through():
    from activities import (
        extract_pdf, ExtractPDFInput,
        call_llm, CallLLMInput,
    )

## These are the workflow's public input/output schema. Temporal serializes dataclasses (typically to/from JSON via its data converter) to pass between client and workflow, and across activity boundaries. Using @dataclass (rather than loose dicts) gives type safety and self-documenting contracts.
@dataclass
class PDFSummaryInput:
    s3_path: str

@dataclass(frozen=True)
class PDFSummaryOutput:
    s3_path: str
    summary: str
    key_risks: str

## This defines exponential backoff retry behavior shared by both activities:
## First retry waits ~3s
## Each subsequent retry interval doubles (backoff_coefficient=2.0): 3s → 6s → 12s → 24s...
## But it's capped at 60s max between retries (maximum_interval)
## It gives up after 4 total attempts (1 initial + 3 retries), then the activity (and workflow, if unhandled) fails
DEFAULT_RETRY_POLICY = RetryPolicy(
    initial_interval=timedelta(seconds=3),
    backoff_coefficient=2.0,
    maximum_interval=timedelta(seconds=60),
    maximum_attempts=4,
)


@workflow.defn ## registers this class as a Temporal workflow type.
class PDFSummaryWorkflow:
    
    @workflow.run ##  marks the entry-point method Temporal calls when the workflow starts. 
    async def run(self, params: PDFSummaryInput) -> PDFSummaryOutput:
        
        # execute: extract_pdf
        ## schedules the extract_pdf activity on a worker and awaits its result. This is a durable call if the workflow's worker process dies mid-execution, 
        # Temporal replays history and picks up exactly where it left off (it won't re-run extract_pdf if it already completed).
        extracted_md = await workflow.execute_activity(
            extract_pdf,
            ## presumably the activity downloads the PDF from S3 and runs OCR/text extraction
            ExtractPDFInput(
                s3_path=params.s3_path
            ),
            retry_policy=DEFAULT_RETRY_POLICY,
            ## the max wall-clock time allowed for a single attempt of this activity to run before Temporal considers it failed/timed out. 
            # PDF extraction (especially OCR on large documents) can be slow, hence the generous 20-minute budget.
            start_to_close_timeout=timedelta(minutes=20),
            ##  the activity is expected to periodically call activity.heartbeat() internally. If Temporal doesn't hear a heartbeat within 30s, it assumes the worker/activity has stalled or died, and can reschedule it (via the retry policy)
            heartbeat_timeout=timedelta(seconds=30),
        )


        # execute: call_llm
        ## Takes the extracted Markdown text and truncates it to the first 5,000 characters before inserting it into the prompt template. 
        prompt = _SUMMARY_PROMPT.format(
            text=extracted_md.markdown_text[:5_000]
        )

        ## Executes the call_llm activity, which presumably sends the prompt to an LLM API (OpenAI, Anthropic, etc.) and returns a response.
        llm_result = await workflow.execute_activity(
            call_llm,
            CallLLMInput(
                prompt=prompt,
                schema_name="PDFSummarySchema"
            ),
            retry_policy=DEFAULT_RETRY_POLICY,
            start_to_close_timeout=timedelta(minutes=5),
            heartbeat_timeout=timedelta(seconds=180),
        )

        workflow.logger.info(
            f"PDFSummarySchema validated in {llm_result.attempts} attempt(s)"
        )

        return PDFSummaryOutput(
            s3_path=params.s3_path,
            summary=llm_result.data.get("summary", ""),
            key_risks=llm_result.data.get("key_risks", "")
        )
    