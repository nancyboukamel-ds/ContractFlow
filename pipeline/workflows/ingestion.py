"""
pipeline/workflows/ingestion.py

Two workflows:

IngestContractWorkflow
  Single PDF → upload → quality check → extract → store → classify → embed
  One child per PDF, started by the batch workflow below.

CUADBatchIngestionWorkflow
  Orchestrates ingestion of all 499 CUAD PDFs in parallel batches.
  Exposes a get_progress Query so you can watch it in real time.

 the parent only knows about fan-out and progress tracking. The child only knows about one PDF.
"""
import asyncio
from dataclasses import dataclass, field
from datetime import timedelta
from pathlib import Path
from typing import Optional

from temporalio import workflow
from temporalio.common import RetryPolicy
from temporalio.exceptions import ApplicationError

with workflow.unsafe.imports_passed_through():
    from activities.storage       import upload_pdf_to_s3,       UploadPDFInput
    from activities.pdf           import assess_pdf_quality,      PDFQualityInput
    from activities.pdf           import extract_pdf_text,        ExtractPDFInput
    from activities.database      import insert_contract,         InsertContractInput
    from activities.database      import insert_clauses_batch,    InsertClauseInput
    from activities.database      import update_contract_status,  UpdateStatusInput
    from activities.classification import classify_clauses,       ClassifyInput
    from activities.embedding     import embed_contract_clauses,  EmbedClausesInput
    from config                   import S3_CUAD_PREFIX


# ── Retry policy ──────────────────────────────────────────────────────────────

DEFAULT_RETRY = RetryPolicy(
    initial_interval    = timedelta(seconds=3),
    backoff_coefficient = 2.0,
    maximum_interval    = timedelta(seconds=60),
    maximum_attempts    = 3,
)


# ── Input / Output types ──────────────────────────────────────────────────────

@dataclass
class IngestContractInput:
    local_pdf_path:      str
    contract_type:       str       # folder name, e.g. "Affiliate_Agreements"
    source:              str       # "cuad" | "user_upload"
    # Ground-truth clauses from CUAD CSV (empty for user uploads)
    cuad_clauses: list = field(default_factory=list)
    # [{"clause_type": str, "text": str, "answer": str}]


@dataclass
class IngestContractOutput:
    contract_id:         str
    filename:            str
    status:              str
    page_count:          int
    quality_score:       float
    extraction_strategy: str
    cuad_clauses_stored: int
    llm_clauses_found:   int
    clauses_embedded:    int


@dataclass
class BatchIngestionInput:
    contracts:   list         # list of IngestContractInput dicts
    concurrency: int = 5      # parallel child workflows at a time


@dataclass
class BatchIngestionOutput:
    total:     int
    succeeded: int
    failed:    int


# ── IngestContractWorkflow ────────────────────────────────────────────────────

@workflow.defn
class IngestContractWorkflow:
    """
    Full ingestion pipeline for one PDF contract.

    Steps:
      1. Upload to S3
      2. Assess PDF quality → choose extraction strategy
      3. Extract text (direct / OCR / mixed)
      4. Insert contract row into Postgres
      5. Insert CUAD ground-truth clauses (if source=cuad)
      6. Run LLM clause classification → insert LLM-detected clauses
      7. Embed all clauses into pgvector
    """

    @workflow.run
    async def run(self, params: IngestContractInput) -> IngestContractOutput:

        filename = Path(params.local_pdf_path).name
        title    = Path(params.local_pdf_path).stem
        workflow.logger.info(f"Ingesting: {filename}")

        contract_id: Optional[str] = None

        try:
            # ── Step 1: Upload to S3 ──────────────────────────────────────
            safe_type = params.contract_type.replace(" ", "_")
            s3_key    = f"{S3_CUAD_PREFIX}/{safe_type}/{filename}"

            ## If the worker that uploaded it crashes, a different worker can pick up the workflow and download from S3.
            upload = await workflow.execute_activity(
                upload_pdf_to_s3,
                UploadPDFInput(
                    local_path=params.local_pdf_path,
                    s3_key=s3_key,
                ),
                start_to_close_timeout=timedelta(minutes=5),
                heartbeat_timeout=timedelta(seconds=30),
                retry_policy=DEFAULT_RETRY,
            )

            # ── Step 2: Quality check ─────────────────────────────────────
            ## Returns strategy, quality_score, page_count, warning. The strategy flows directly into step 3.
            quality = await workflow.execute_activity(
                assess_pdf_quality,
                PDFQualityInput(local_path=params.local_pdf_path),
                start_to_close_timeout=timedelta(minutes=2),
                heartbeat_timeout=timedelta(seconds=30),
                retry_policy=DEFAULT_RETRY,
            )
            workflow.logger.info(
                f"Quality: strategy={quality.strategy}, "
                f"score={quality.quality_score:.2f}, "
                f"pages={quality.page_count}"
            )

            # ── Step 3: Extract text ──────────────────────────────────────
            extracted = await workflow.execute_activity(
                extract_pdf_text,
                ExtractPDFInput(
                    local_path=params.local_pdf_path,
                    strategy=quality.strategy,
                    batch_size=5,
                ),
                start_to_close_timeout=timedelta(minutes=20),
                heartbeat_timeout=timedelta(seconds=45),
                retry_policy=DEFAULT_RETRY,
            )
            workflow.logger.info(
                f"Extracted {extracted.char_count:,} chars "
                f"via {extracted.strategy_used}"
            )

            # ── Step 4: Insert contract into Postgres ─────────────────────
            ## This is where results from steps 1-3 converge the S3 path from upload, the text from extraction, the quality metadata all go into one contract row. contract_id is returned and used in every subsequent step.
            insert_result = await workflow.execute_activity(
                insert_contract,
                InsertContractInput(
                    filename            = filename,
                    title               = title,
                    contract_type       = params.contract_type,
                    source              = params.source,
                    s3_path             = upload.s3_path,
                    full_text           = extracted.full_text,
                    page_count          = extracted.page_count,
                    char_count          = extracted.char_count,
                    quality_score       = quality.quality_score,
                    extraction_strategy = extracted.strategy_used,
                    quality_warning     = quality.warning,
                ),
                start_to_close_timeout=timedelta(seconds=30),
                retry_policy=DEFAULT_RETRY,
            )
            contract_id = insert_result.contract_id

            # ── Step 5: Insert CUAD ground-truth clauses ──────────────────
            ## Some CUAD rows have answer=Yes but empty text 
            ## Filtering these out prevents inserting empty clause rows that would break the embedding step.
            cuad_clause_inputs = [
                InsertClauseInput(
                    contract_id     = contract_id,
                    clause_type     = c["clause_type"],
                    text            = c["text"][:2000],
                    answer          = c.get("answer"),
                    start_char      = c.get("start_char"),
                    confidence      = 1.0,          # human-labeled = 100%
                    is_cuad_labeled = True,
                )
                for c in params.cuad_clauses
                if c.get("text", "").strip()
            ]

            cuad_stored = 0
            if cuad_clause_inputs:
                cuad_stored = await workflow.execute_activity(
                    insert_clauses_batch,
                    cuad_clause_inputs,
                    start_to_close_timeout=timedelta(seconds=30),
                    retry_policy=DEFAULT_RETRY,
                )

            # ── Step 6: LLM clause classification ────────────────────────
            llm_detected = await workflow.execute_activity(
                classify_clauses,
                ClassifyInput(
                    contract_id   = contract_id,
                    contract_text = extracted.full_text[:8000],
                    contract_type = params.contract_type,
                ),
                start_to_close_timeout=timedelta(minutes=3),
                heartbeat_timeout=timedelta(seconds=60),
                retry_policy=DEFAULT_RETRY,
            )

            # Only store clauses the LLM found and CUAD didn't already label
            cuad_types = {c["clause_type"] for c in params.cuad_clauses}
            llm_clause_inputs = [
                InsertClauseInput(
                    contract_id     = contract_id,
                    clause_type     = c.clause_type,
                    text            = c.excerpt,
                    answer          = "Yes" if c.present else "No",
                    start_char      = None,
                    confidence      = c.confidence,
                    is_cuad_labeled = False,
                )
                for c in llm_detected
                if c.present
                and c.clause_type not in cuad_types   # don't duplicate
                and c.excerpt.strip() ##  don't store empty excerpts
            ]

            llm_stored = 0
            if llm_clause_inputs:
                llm_stored = await workflow.execute_activity(
                    insert_clauses_batch,
                    llm_clause_inputs,
                    start_to_close_timeout=timedelta(seconds=30),
                    retry_policy=DEFAULT_RETRY,
                )

            # ── Step 7: Embed clauses into pgvector ───────────────────────
            ## Runs after both CUAD and LLM clauses are inserted 
            ## embeds everything in one pass
            embed_result = await workflow.execute_activity(
                embed_contract_clauses,
                EmbedClausesInput(contract_id=contract_id),
                start_to_close_timeout=timedelta(minutes=10),
                heartbeat_timeout=timedelta(seconds=60),
                retry_policy=DEFAULT_RETRY,
            )

            # Mark contract as ready
            await workflow.execute_activity(
                update_contract_status,
                UpdateStatusInput(contract_id=contract_id, status="ready"),
                start_to_close_timeout=timedelta(seconds=10),
                retry_policy=DEFAULT_RETRY,
            )

            workflow.logger.info(
                f"Done: {filename} | "
                f"cuad={cuad_stored} llm={llm_stored} "
                f"embedded={embed_result.clauses_embedded}"
            )

            return IngestContractOutput(
                contract_id          = contract_id,
                filename             = filename,
                status               = "ready",
                page_count           = extracted.page_count,
                quality_score        = quality.quality_score,
                extraction_strategy  = extracted.strategy_used,
                cuad_clauses_stored  = cuad_stored,
                llm_clauses_found    = llm_stored,
                clauses_embedded     = embed_result.clauses_embedded,
            )

        except Exception as e:
            workflow.logger.error(f"Failed to ingest {filename}: {e}")
            ## If the failure happened in step 4 (insert_contract), contract_id is still None the row doesn't exist yet, so there's nothing to update. The check prevents a second error inside the error handler.
            if contract_id:
                await workflow.execute_activity(
                    update_contract_status,
                    UpdateStatusInput(
                        contract_id   = contract_id,
                        status        = "failed",
                        error_message = str(e)[:500],
                    ),
                    start_to_close_timeout=timedelta(seconds=10),
                    retry_policy=DEFAULT_RETRY,
                )
            ## Temporal sees the failure and records it in workflow history.
            raise


# ── CUADBatchIngestionWorkflow ─────────────────────────────────────────────────

@workflow.defn
class CUADBatchIngestionWorkflow:
    """
    Fan-out ingestion of all CUAD contracts.
    Processes in batches of `concurrency` to avoid overwhelming the worker pool.
    Exposes get_progress query for live monitoring.
    """

    def __init__(self):
        self._total   = 0
        self._done    = 0
        self._failed  = 0
        self._status  = "starting"
        self._current_batch = 0

    ## This is what run_ingestion.py polls every 15 seconds to print progress. No database query needed the workflow holds this in memory.
    @workflow.query
    def get_progress(self) -> dict:
        return {
            "status":        self._status,
            "total":         self._total,
            "done":          self._done,
            "failed":        self._failed,
            "current_batch": self._current_batch,
            "pct":           round(self._done / self._total * 100)
                             if self._total else 0,
        }

    @workflow.run
    async def run(self, params: BatchIngestionInput) -> BatchIngestionOutput:

        contracts        = params.contracts
        concurrency      = params.concurrency
        self._total      = len(contracts)
        self._status     = "ingesting"
        workflow_id      = workflow.info().workflow_id
        task_queue       = workflow.info().task_queue

        workflow.logger.info(
            f"Batch ingestion: {self._total} contracts, "
            f"concurrency={concurrency}"
        )

        """
        Why sliding window instead of all 499 at once?
        Starting 499 child workflows simultaneously would flood the task queue and overwhelm
        the worker. With concurrency=5, you process 5 contracts in parallel, wait for them all to
        finish, then start the next 5. This keeps the worker pool busy without overloading it.
        """
        # Process in sliding windows of `concurrency` child workflows
        for batch_start in range(0, len(contracts), concurrency):
            batch = contracts[batch_start : batch_start + concurrency]
            self._current_batch = batch_start // concurrency + 1

            # Start all children in this batch simultaneously

            handles = await asyncio.gather(*[
                workflow.start_child_workflow(
                    IngestContractWorkflow.run,
                    IngestContractInput(**contract),
                    id=f"{workflow_id}-{batch_start + j}",
                    task_queue=task_queue,
                )
                for j, contract in enumerate(batch)
            ])

            # Wait for all of them, tolerating individual failures
            ## Without return_exceptions=True, one failed child would cancel all others with it, failures are captured as exceptions in the results list, counted, and the batch continues.
            results = await asyncio.gather(*handles, return_exceptions=True)

            for result in results:
                if isinstance(result, Exception):
                    self._failed += 1
                    workflow.logger.warning(f"Child workflow failed: {result}")
                else:
                    self._done += 1

            workflow.logger.info(
                f"Batch {self._current_batch} done | "
                f"progress: {self._done}/{self._total} "
                f"({round(self._done/self._total*100)}%)"
            )

        self._status = "completed"
        workflow.logger.info(
            f"Batch ingestion complete: "
            f"succeeded={self._done}, failed={self._failed}"
        )

        return BatchIngestionOutput(
            total     = self._total,
            succeeded = self._done,
            failed    = self._failed,
        )