import os
import math
import tempfile
import json_repair
from pathlib import Path
from dataclasses import dataclass,field
from typing import Type

import boto3
import fitz                  # PyMuPDF — page-by-page extraction
import pymupdf4llm
from dotenv import load_dotenv
from openai import OpenAI
from pydantic import BaseModel,ValidationError
from temporalio import activity
from temporalio.exceptions import ApplicationError
from schemas import SCHEMA_REGISTRY

load_dotenv(Path(__file__).parent / ".env")

# ── Dataclasses ───────────────────────────────────────────────────────────────

##  Temporal's way of passing typed data into and out of activities. 
@dataclass
class ExtractPDFInput:
    s3_path: str
    batch_size: int = 2

@dataclass
class ExtractPDFOutput:
    s3_path: str
    markdown_text: str
    page_count: int

@dataclass
class CallLLMInput:
    prompt: str
    schema_name:str   # key into SCHEMA_REGISTRY, e.g. "PDFSummarySchema"

@dataclass
class CallLLMOutput:
    content:str # raw string always available
    data: dict # validated dict matching the requested Pydantic schema
    attempts:int #  how many LLM calls it took to get valid output

# ── S3 helper ────────────────────────────────────────────────────────────────

## Creates a fresh boto3 S3 client each time it's called. The endpoint_url is what makes it work with
# iDrive E2 instead of real AWS — boto3 supports any S3-compatible storage by pointing it at a different endpoint.

def get_s3_client():
    return boto3.client(
        "s3",
        aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
        region_name=os.environ["AWS_REGION"],
        endpoint_url=os.environ["AWS_S3_ENDPOINT_URL"],
    )


def parse_s3_path(s3_path: str):
    s3_path_no_scheme = s3_path.replace("s3://", "")
    bucket, _, key =  s3_path_no_scheme.partition("/")
    return bucket, key

# ── Activity 1: Extract PDF from S3 ──────────────────────────────────────────

## @activity.defn registers this function with Temporal. The function name extract_pdf becomes the activity's identifier this is what the workflow uses to dispatch it.
## It's async because the Temporal Python SDK runs on asyncio, even though the actual work here (boto3 download, pymupdf) is synchronous. 
@activity.defn
async def extract_pdf(params: ExtractPDFInput) -> ExtractPDFOutput:
    
    activity.logger.info(f"Starting extraction: {params.s3_path}")
    
    ## This fires immediately, before any I/O. Two reasons: it proves the activity started (useful for debugging), and it seeds the heartbeat details so if the worker dies during the download, the retry can see stage: downloading and know where it failed.
    activity.heartbeat({
        "stage": "downloading",
        "s3_path": params.s3_path,
        "pages_done": 0,
        "chars_extracted": 0,
    })

    s3_client = get_s3_client()
    bucket, key = parse_s3_path(params.s3_path)

    filename = Path(key).name  # "files/paper.pdf" → "paper.pdf"
    TEMP_DIR = os.environ["TEMP_DIR"]

    local_path = str(Path(TEMP_DIR) / filename) ## "/tmp/pdf-pipeline/paper.pdf"

    s3_client.download_file(
        bucket,
        key,
        local_path
    )

    ## Opens the PDF with PyMuPDF just to read the page count. This is cheap — it doesn't load all the pages into memory, just reads the PDF metadata. 
    ## The count is needed to calculate num_batches and track progress percentages.
    doc = fitz.open(local_path)
    total_pages = doc.page_count

    activity.logger.info(f"Downloaded {total_pages}-page PDF: {params.s3_path}")

    all_text_chunks = []
    total_chars_num = 0
    num_batches = math.ceil(total_pages / params.batch_size)

    for batch_idx in range(num_batches):

        start_page = batch_idx * params.batch_size
        end_page =  min(start_page + params.batch_size, total_pages)

        ## pymupdf4llm.to_markdown() converts PDF pages to LLM-friendly markdown it handles tables, headers, and layout much better than plain text extraction
        ## The pages= argument lets you extract a subset rather than the whole document.
        batch_md = pymupdf4llm.to_markdown(
            local_path,
            pages=list(range(start_page, end_page)),
        )

        all_text_chunks.append(batch_md)
        total_chars_num += len(batch_md)

        ## if Temporal doesn't receive a heartbeat within 30 seconds, it assumes the worker is dead and retries the activity. 
        activity.heartbeat({
            "stage":           "extracting",
            "s3_path":         params.s3_path,
            "pages_done":      end_page,
            "total_pages":     total_pages,
            "batch":           f"{start_page + 1}–{end_page}",
            "chars_extracted": total_chars_num,
            "progress_pct":    round(end_page / total_pages * 100),
        })

 
    ## All batch chunks are joined with double newlines to preserve document structure. 
    full_md = "\n\n".join(all_text_chunks)

    ## The final heartbeat before returning marks completion in the heartbeat history useful for post-mortem debugging
    activity.heartbeat({
        "stage": "done",
        "s3_path": params.s3_path,
        "pages_done": total_pages,
        "chars_extracted": total_chars_num,
    })

    return ExtractPDFOutput(
        s3_path=params.s3_path,
        markdown_text=full_md,
        page_count=total_pages,
    )

def _pydantic_to_json_schema_format(schema_cls: Type[BaseModel]) -> dict:
    """
    Converts a Pydantic model into the OpenAI 'strict' Structured Outputs format.
    Strict mode requires additionalProperties: false and all fields marked required
    (this is an OpenAI API constraint, not a Pydantic one) — we patch the raw
    JSON schema to satisfy it.
    """
    schema = schema_cls.model_json_schema()
    schema["additionalProperties"] = False

    def _enforce_strict(node: dict):
        if node.get("type") == "object" and "properties" in node:
            node["required"] = list(node["properties"].keys())
            node["additionalProperties"] = False
            for sub in node["properties"].values():
                _enforce_strict(sub)
        elif node.get("type") == "array" and "items" in node:
            _enforce_strict(node["items"])
        # walk $defs too (Pydantic puts nested models here)
        for defn in node.get("$defs", {}).values():
            _enforce_strict(defn)

    _enforce_strict(schema)

    return {
        "type": "json_schema",
        "json_schema": {
            "name": schema_cls.__name__,
            "schema": schema,
            "strict": True,
        },
    }


# ── Activity 2: Call the LLM via OpenRouter ───────────────────────────────────

@activity.defn
async def call_llm(params: CallLLMInput) -> CallLLMOutput:
    activity.logger.info("Calling LLM")
    ## Heartbeats immediately with the prompt length useful for spotting if you're accidentally sending enormous prompts.
    activity.heartbeat({"stage": "calling_llm",
                         "prompt_chars": len(params.prompt)})
    import os
    key = os.environ.get("OPENROUTER_API_KEY", "NOT FOUND")
    activity.logger.info(f"API key present: {bool(key)}, starts with: {key[:10]}")
    
    ## OpenRouter is a proxy that gives you access to many LLM providers (OpenAI, Anthropic, Mistral, etc.) through one API key and one endpoint. The OpenAI SDK works unchanged you just swap the base_url.
    llm_client = OpenAI(
        api_key=os.environ["OPENROUTER_API_KEY"],
        base_url="https://openrouter.ai/api/v1",
    )

    schema = SCHEMA_REGISTRY.get(params.schema_name) if params.schema_name else None
    max_attempts = 3 if schema else 1

    for attempt in range(1, max_attempts + 1):
        response = llm_client.chat.completions.create(
            model=os.environ.get("OPENROUTER_MODEL", "openai/gpt-4o-mini"),
            messages=[{"role": "user", "content": params.prompt}],
            max_tokens=8000,
        )

        content = response.choices[0].message.content
        activity.logger.info(f"LLM returned {len(content)} chars (attempt {attempt})")

        parsed = json_repair.loads(content)

        if schema:
            try:
                validated = schema(**parsed)
                return CallLLMOutput(
                    content=content,
                    data=validated.model_dump(),
                    attempts=attempt,
                )
            except Exception as e:
                activity.logger.warning(f"Schema validation failed attempt {attempt}: {e}")
                activity.heartbeat({"stage": "retrying_llm", "attempt": attempt, "error": str(e)})
                if attempt == max_attempts:
                    raise ApplicationError(f"LLM failed schema validation after {max_attempts} attempts: {e}")
        else:
            return CallLLMOutput(content=content, data=parsed, attempts=attempt)