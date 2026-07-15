"""
pipeline/activities/storage.py
Upload and download PDFs to/from S3-compatible storage (iDrive E2).

This file contains two activities that handle moving PDFs between your local disk and S3:
uploading CUAD PDFs during ingestion, and downloading them back when needed for processing.

Why S3 at all?
S3 is the shared storage layer that any worker on any machine can access. It also means your processed PDFs are safely stored even if your local machine's disk is wiped.
"""
import os
from dataclasses import dataclass
from pathlib import Path

import boto3
from temporalio import activity

from config import S3_BUCKET, TEMP_DIR


# ── Helpers ───────────────────────────────────────────────────────────────────
"""
Why a function instead of a module-level client?
Unlike the database pool, an S3 client is cheap to create no network connection until you actually make a request. 
Creating it fresh each time the activity runs means credentials are always read from the current environment, which matters if you rotate keys.
It also avoids potential thread-safety issues with a shared boto3 client across async activities.
"""

def _s3_client():
    return boto3.client(
        "s3",
        aws_access_key_id     = os.environ["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key = os.environ["AWS_SECRET_ACCESS_KEY"],
        region_name           = os.environ["AWS_REGION"],
        ## This is what makes boto3 work with iDrive E2 instead of AWS S3 by routing it to iDrive bucket
        endpoint_url          = os.environ["AWS_S3_ENDPOINT_URL"],
    )


# ── Dataclasses ───────────────────────────────────────────────────────────────

@dataclass
class UploadPDFInput:
    local_path: str
    s3_key: str            # e.g. "cuad/Affiliate_Agreement/contract.pdf"


@dataclass
class UploadPDFOutput:
    s3_path: str
    size_bytes: int


@dataclass
class DownloadPDFInput:
    s3_path: str           # e.g. "s3://bucket/cuad/..."


@dataclass
class DownloadPDFOutput:
    local_path: str


# ── Activities ────────────────────────────────────────────────────────────────

## Takes a local PDF path and an S3 key, uploads it, returns the full s3:// path.
@activity.defn
async def upload_pdf_to_s3(params: UploadPDFInput) -> UploadPDFOutput:
    """Upload a local PDF to S3."""
    ## The heartbeat fires immediately so Temporal knows the activity is alive and working. If the heartbeat came after, Temporal might time out the activity during a legitimate upload.
    ## cuz The upload is the potentially slow operation a large PDF over a slow connection could take 30+ seconds
    activity.heartbeat({"stage": "uploading", "key": params.s3_key})

    s3   = _s3_client()
    size = Path(params.local_path).stat().st_size

    s3.upload_file(
        params.local_path,
        S3_BUCKET,
        params.s3_key,
        ## application/pdf: Without it, S3 stores the file with application/octet-stream a generic binary type. 
        ## Setting the correct MIME type means if you later build a frontend that lets users download contracts, browsers will open them correctly in a PDF viewer instead of prompting a download.
        ExtraArgs={"ContentType": "application/pdf"},
    )
    
    ## cuz  S3 keys with spaces cause problems in URLs.
    s3_path = f"s3://{S3_BUCKET}/{params.s3_key}"
    activity.logger.info(f"Uploaded {size / 1024:.1f} KB → {s3_path}")

    return UploadPDFOutput(s3_path=s3_path, size_bytes=size)

## The reverse of upload takes an s3:// path, downloads to TEMP_DIR, returns the local path.
@activity.defn
async def download_pdf_from_s3(params: DownloadPDFInput) -> DownloadPDFOutput:
    """Download a PDF from S3 to a local temp file."""
    activity.heartbeat({"stage": "downloading", "s3_path": params.s3_path})

    # Parse s3://bucket/key
    without_scheme = params.s3_path.replace("s3://", "")
    bucket, _, key = without_scheme.partition("/")

    filename   = Path(key).name
    # Defined in config as /tmp/redline-pipeline/
    local_path = str(TEMP_DIR / filename)

    _s3_client().download_file(bucket, key, local_path)
    activity.logger.info(f"Downloaded → {local_path}")

    return DownloadPDFOutput(local_path=local_path)