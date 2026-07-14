"""
FastAPI Client App — PDF Processing via Temporal

Educational example showing how a client API submits work to Temporal
and waits for the result, without doing any heavy processing itself.

The client app is completely decoupled from the worker code — it only
needs the Temporal SDK to talk to the Temporal server. Workflows are
referenced by string name, and inputs/outputs are plain dicts (JSON).

Flow:
  POST /process-pdf
    └─► connect to Temporal server
    └─► start workflow by name (no worker code imported)
    └─► wait for result (as plain dict)
    └─► return response to caller

Request:

curl -X POST http://localhost:5000/process-pdf/execute \
  -H "Content-Type: application/json" \
  -d '{"s3_path": "s3://temporal-dev/files/cisco-88xx-user-guide.pdf"}'


curl -X POST http://localhost:5000/process-pdf/start \
  -H "Content-Type: application/json" \
  -d '{"s3_path": "s3://temporal-dev/files/cisco-88xx-user-guide.pdf"}'

curl http://localhost:5000/workflow/status/pdf-pipeline-158e2b08-d378-4c77-9730-9480ecca1b27


curl -X POST http://localhost:5000/contract-review/start \
  -H "Content-Type: application/json" \
  -d '{
    "s3_paths": [
      "s3://temporal-dev/legal-docs/vendor-service-agreement.pdf",
      "s3://temporal-dev/legal-docs/nda-innovate-consultpro.pdf",
      "s3://temporal-dev/legal-docs/software-license-globalsoft.pdf"
    ]
  }'

  
curl http://localhost:5000/contract-review/contract-review-e66c2d8e-be77-4bac-8072-cb545d316f10/status

curl http://localhost:5000/contract-review/contract-review-e66c2d8e-be77-4bac-8072-cb545d316f10/report


curl -X POST http://localhost:5000/contract-review/contract-review-e66c2d8e-be77-4bac-8072-cb545d316f10/assign \
  -H "Content-Type: application/json" \
  -d '{"name": "Abu Bakr"}'

curl -X POST http://localhost:5000/contract-review/contract-review-e66c2d8e-be77-4bac-8072-cb545d316f10/revise \
  -H "Content-Type: application/json" \
  -d '{"feedback": "Please write the report in Arabic."}'

curl http://localhost:5000/contract-review/contract-review-e66c2d8e-be77-4bac-8072-cb545d316f10/approve


"""

import os
import uuid

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from dotenv import load_dotenv
from temporalio.client import Client
from temporalio.client import WorkflowExecutionStatus as WES

load_dotenv()

TEMPORAL_HOST      = os.environ["TEMPORAL_HOST"]
TEMPORAL_NAMESPACE = os.environ["TEMPORAL_NAMESPACE"]
TEMPORAL_PDF_PROCESS_TASK_QUEUE = os.environ["TEMPORAL_PDF_PROCESS_TASK_QUEUE"]
TEMPORAL_CONTRACT_REVIEW_TASK_QUEUE = os.environ["TEMPORAL_CONTRACT_REVIEW_TASK_QUEUE"]

app = FastAPI(
    title="PDF Extraction Client",
    description="Submits PDF processing jobs to Temporal and returns the result.",
    version="1.0.0",
)

class ProcessPDFRequest(BaseModel):
    s3_path: str

class ProcessPDFExecuteResponse(BaseModel):
    workflow_id: str
    results: dict

class ProcessPDFStartResponse(BaseModel):
    workflow_id: str


class StartReviewRequest(BaseModel):
    s3_paths: list[str]
    max_revisions: int = 2

class AssignRequest(BaseModel):
    name: str

class ReviseRequest(BaseModel):
    feedback: str


async def get_temporal_client() -> Client:
    return await Client.connect(
        TEMPORAL_HOST,
        namespace=TEMPORAL_NAMESPACE,
    )

# ── Routes ────────────────────────────────────────────────────────────────────

@app.get("/health")
async def health():
    return {"status": "ok"}


# ====================
# PDF PROCESS
# ====================

@app.post("/process-pdf/execute", response_model=ProcessPDFExecuteResponse)
async def process_pdf(request: ProcessPDFRequest):
    
    workflow_id = f"pdf-pipeline-{uuid.uuid4()}"

    client = await get_temporal_client()

    results = await client.execute_workflow(
        "PDFPipelineWorkflow",
        args=[
            {
                "s3_path": request.s3_path,
            }
        ],
        id=workflow_id,
        task_queue=TEMPORAL_PDF_PROCESS_TASK_QUEUE,
        result_type=dict,
    )

    return ProcessPDFExecuteResponse(
        workflow_id=workflow_id,
        results=results
    )

@app.post("/process-pdf/start", response_model=ProcessPDFStartResponse)
async def process_pdf(request: ProcessPDFRequest):
    
    workflow_id = f"pdf-pipeline-{uuid.uuid4()}"

    client = await get_temporal_client()

    results = await client.start_workflow(
        "PDFPipelineWorkflow",
        args=[
            {
                "s3_path": request.s3_path,
            }
        ],
        id=workflow_id,
        task_queue=TEMPORAL_PDF_PROCESS_TASK_QUEUE,
        result_type=dict,
    )

    return ProcessPDFStartResponse(
        workflow_id=workflow_id,
    )

@app.get("/workflow/status/{workflow_id}")
async def get_workflow_status(workflow_id: str):

    client = await get_temporal_client()

    handle = client.get_workflow_handle(
        workflow_id,
        result_type=dict
    )

    desc = await handle.describe()

    try:
        result = await handle.result()
    except:
        result = None

    workflow_status = desc.status

    return {
        "workflow_id": workflow_id,
        "workflow_status": workflow_status.name,
        "workflow_result": result
    }


# ====================
# CONTRACT REVIEW
# ====================

@app.post("/contract-review/start")
async def start_contract_review(request: StartReviewRequest):
    
    workflow_id = f"contract-review-{uuid.uuid4()}"

    client = await get_temporal_client()

    await client.start_workflow(
        "ContractReviewWorkflow",
        args=[{
            "s3_paths": request.s3_paths,
            "max_revisions": request.max_revisions
        }],
        id=workflow_id,
        task_queue=TEMPORAL_CONTRACT_REVIEW_TASK_QUEUE,
    )

    return {"workflow_id": workflow_id}


@app.get("/contract-review/{workflow_id}/status")
async def get_review_status(workflow_id: str):

    """Temporal execution status + brief workflow state (Query)."""
    
    client = await get_temporal_client()
    handle = client.get_workflow_handle(workflow_id)
    desc = await handle.describe()

    workflow_state = None
    if desc.status == WES.RUNNING:
        try:
            workflow_state = await handle.query("get_status", result_type=dict)
        except Exception as e:
            workflow_state = {"error": str(e)}
    
    return {
        "workflow_id": workflow_id,
        "execution_status": desc.status.name,
        "workflow_state": workflow_state,
    }

@app.get("/contract-review/{workflow_id}/report")
async def get_review_report(workflow_id: str):

    """Temporal execution report + brief workflow state (Query)."""
    
    client = await get_temporal_client()
    handle = client.get_workflow_handle(workflow_id)
    desc = await handle.describe()

    workflow_report = None
    if desc.status == WES.RUNNING:
        try:
            workflow_report = await handle.query("get_report", result_type=dict)
        except Exception as e:
            workflow_report = {"error": str(e)}
    
    return {
        "workflow_id": workflow_id,
        "execution_report": desc.status.name,
        "workflow_report": workflow_report,
    }

@app.post("/contract-review/{workflow_id}/assign")
async def assign_reviewer(workflow_id: str, request: AssignRequest):

    client = await get_temporal_client()
    handle = client.get_workflow_handle(workflow_id)

    await handle.signal(
        "assign_reviewer", request.name
    )

    return {"status": "ok", 
            "message": f"Reviewer '{request.name}' assigned."}


@app.post("/contract-review/{workflow_id}/revise")
async def submit_revise(workflow_id: str, request: ReviseRequest):

    client = await get_temporal_client()
    handle = client.get_workflow_handle(workflow_id)

    result = await handle.execute_update(
        "submit_decision", args=[
            "revise", request.feedback
        ]
    )

    return {"ok": True, "message": result}


@app.get("/contract-review/{workflow_id}/approve")
async def submit_approve(workflow_id: str):

    client = await get_temporal_client()
    handle = client.get_workflow_handle(workflow_id)

    result = await handle.execute_update(
        "submit_decision", args=[
            "approve", ""
        ]
    )

    return {"ok": True, "message": result}