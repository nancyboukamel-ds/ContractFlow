import asyncio
import logging
import os
from pathlib import Path
from dotenv import load_dotenv
## connects to the Temporal server and can start workflows, send signals, run queries. It's a thin network client, it doesn't execute anything itself.
from temporalio.client import Client
##  the process that executes workflow and activity code. It polls a task queue and runs whatever Temporal tells it to run.
from temporalio.worker import Worker

from activities import extract_pdf, call_llm
from child_workflow import PDFSummaryWorkflow
from parent_workflow import ContractReviewWorkflow



TEMPORAL_HOST       = os.environ["TEMPORAL_HOST"]
TEMPORAL_NAMESPACE  = os.environ["TEMPORAL_NAMESPACE"]
TEMPORAL_TASK_QUEUE = os.environ["TEMPORAL_TASK_QUEUE"]

async def main():

    ## Opens a persistent gRPC connection to the Temporal server. The await is because it does a real network handshake.
    #  This client is then shared with the worker — the worker uses it internally to communicate task completion back to the server.
    temporal_client = await Client.connect(TEMPORAL_HOST, 
                                           namespace=TEMPORAL_NAMESPACE)
    
    ## task_queue — which queue to poll. Only tasks on this queue will be picked up by this worker.
    ## if you set a custom name) to match incoming workflow tasks. If a workflow arrives on the queue that isn't in this list, the worker ignores it.
    ## 
    worker = Worker(
        temporal_client,
        task_queue=TEMPORAL_TASK_QUEUE,
        workflows=[ContractReviewWorkflow, PDFSummaryWorkflow],
        activities=[extract_pdf, call_llm],
    )

    print(f"Worker running on: '{TEMPORAL_TASK_QUEUE}'")
    ## worker.run() starts the polling loop and never returns (until the process is killed or it crashes). 
    await worker.run()


if __name__ == "__main__":
    asyncio.run(main())