"""
pipeline/worker.py
Temporal worker that registers all pipeline workflows and activities.

It's the process that actually executes everything. Without it running, no workflow moves forward.

Why load dotenv at the very top before any other imports?
Several imports below — config.py, activities/ read os.environ at import time. 
If load_dotenv runs after those imports, the environment variables aren't set yet when the 
modules try to read them. Loading first guarantees every module sees the correct values.

Why must every workflow and activity be explicitly listed?
Temporal dispatches tasks by name when the server sends "run assess_pdf_quality",
the worker looks up that name in its registry. If it's not listed, the worker ignores the task 
and it sits on the queue forever. This is a common bug forgetting to register a new activity
after writing it.
"""
import asyncio
import logging
from pathlib import Path

from dotenv import load_dotenv
load_dotenv(Path(__file__).parent / ".env")

from temporalio.client import Client
from temporalio.worker import Worker

from config import TEMPORAL_HOST, TEMPORAL_NAMESPACE, TEMPORAL_TASK_QUEUE

from workflows.ingestion import (
    IngestContractWorkflow,
    CUADBatchIngestionWorkflow,
)
from activities.storage        import upload_pdf_to_s3, download_pdf_from_s3
from activities.pdf            import assess_pdf_quality, extract_pdf_text
from activities.database       import (
    insert_contract,
    insert_clauses_batch,
    update_contract_status,
)
from activities.classification import classify_clauses
from activities.embedding      import embed_contract_clauses, search_clauses

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s — %(message)s",
)


async def main() -> None:
    print(f"Connecting to Temporal at {TEMPORAL_HOST}...")
    ## Temporal puts tasks on "pipeline-queue". The worker polls "pipeline-queue". They find each other through the queue name 
    client = await Client.connect(TEMPORAL_HOST, namespace=TEMPORAL_NAMESPACE)

    worker = Worker(
        client,
        task_queue=TEMPORAL_TASK_QUEUE,
        workflows=[
            IngestContractWorkflow,
            CUADBatchIngestionWorkflow,
        ],
        activities=[
            # Storage
            upload_pdf_to_s3,
            download_pdf_from_s3,
            # PDF
            assess_pdf_quality,
            extract_pdf_text,
            # Database
            insert_contract,
            insert_clauses_batch,
            update_contract_status,
            # LLM
            classify_clauses,
            # Embeddings
            embed_contract_clauses,
            search_clauses,
        ],
    )

    print(f"Pipeline worker running on '{TEMPORAL_TASK_QUEUE}'")
    print(f"Registered workflows:")
    print(f"  - IngestContractWorkflow")
    print(f"  - CUADBatchIngestionWorkflow")
    print(f"Registered activities:")
    print(f"  - upload_pdf_to_s3, download_pdf_from_s3")
    print(f"  - assess_pdf_quality, extract_pdf_text")
    print(f"  - insert_contract, insert_clauses_batch, update_contract_status")
    print(f"  - classify_clauses")
    print(f"  - embed_contract_clauses, search_clauses")
    print()

    await worker.run()


if __name__ == "__main__":
    asyncio.run(main())