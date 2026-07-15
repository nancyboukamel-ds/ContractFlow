"""
pipeline/config.py
Central configuration for the Redline data pipeline.
All paths, constants and environment variables in one place.
"""
import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv(Path(__file__).parent / ".env")

# ── CUAD dataset ──────────────────────────────────────────────────────────────

# Root of the downloaded CUAD_v1 folder
# Set CUAD_ROOT in your .env, e.g. CUAD_ROOT=/home/nancybk/durable-pdf-pipeline/CUAD_v1
CUAD_ROOT = Path(os.environ.get("CUAD_ROOT", 
    Path(__file__).parent.parent / "CUAD_v1"))

CUAD_CSV        = CUAD_ROOT / "master_clauses.csv"
CUAD_PDF_ROOT   = CUAD_ROOT / "full_contract_pdf"
CUAD_PDF_PARTS  = [
    CUAD_PDF_ROOT / "Part_I",
    CUAD_PDF_ROOT / "Part_II",
    CUAD_PDF_ROOT / "Part_III",
]

# ── S3 ────────────────────────────────────────────────────────────────────────

S3_BUCKET        = os.environ.get("S3_BUCKET", "temporal-dev")
S3_CUAD_PREFIX   = "cuad"       # s3://bucket/cuad/ContractType/file.pdf
S3_UPLOAD_PREFIX = "uploads"    # s3://bucket/uploads/tenant/file.pdf

# ── Postgres ──────────────────────────────────────────────────────────────────

DATABASE_URL = os.environ.get(
    "DATABASE_URL",
    "postgresql://redline:redline123@localhost:5432/redline"
)

# ── Temporal ──────────────────────────────────────────────────────────────────

TEMPORAL_HOST       = os.environ.get("TEMPORAL_HOST", "localhost:7233")
TEMPORAL_NAMESPACE  = os.environ.get("TEMPORAL_NAMESPACE", "default")
TEMPORAL_TASK_QUEUE = "pipeline-queue"

# ── LLM ───────────────────────────────────────────────────────────────────────

OPENROUTER_API_KEY = os.environ.get("OPENROUTER_API_KEY", "")
OPENROUTER_MODEL   = os.environ.get("OPENROUTER_MODEL", "openai/gpt-4o-mini")
EMBEDDING_MODEL    = "text-embedding-3-small"

# ── Local temp dir ────────────────────────────────────────────────────────────

TEMP_DIR = Path(os.environ.get("TEMP_DIR", "/tmp/redline-pipeline"))
TEMP_DIR.mkdir(parents=True, exist_ok=True)

# ── PDF quality thresholds ────────────────────────────────────────────────────

MIN_CHARS_PER_PAGE = 100     # below this → scanned PDF → use OCR
MAX_NON_ASCII_RATIO = 0.3    # above this = garbled text → OCR
MIN_TEXT_PAGE_RATIO = 0.5    # below this fraction of pages have text → mixed

# ── CUAD clause columns ───────────────────────────────────────────────────────
# These are the exact column names from master_clauses.csv
# Each has a matching "[col]-Answer" column (except Notice Period which has a space)

ALL_CLAUSE_COLS = [
    "Document Name",
    "Parties",
    "Agreement Date",
    "Effective Date",
    "Expiration Date",
    "Renewal Term",
    "Notice Period To Terminate Renewal",
    "Governing Law",
    "Most Favored Nation",
    "Competitive Restriction Exception",
    "Non-Compete",
    "Exclusivity",
    "No-Solicit Of Customers",
    "No-Solicit Of Employees",
    "Non-Disparagement",
    "Termination For Convenience",
    "Rofr/Rofo/Rofn",
    "Change Of Control",
    "Anti-Assignment",
    "Revenue/Profit Sharing",
    "Price Restrictions",
    "Minimum Commitment",
    "Volume Restriction",
    "Ip Ownership Assignment",
    "Joint Ip Ownership",
    "License Grant",
    "Non-Transferable License",
    "Affiliate License-Licensor",
    "Affiliate License-Licensee",
    "Unlimited/All-You-Can-Eat-License",
    "Irrevocable Or Perpetual License",
    "Source Code Escrow",
    "Post-Termination Services",
    "Audit Rights",
    "Uncapped Liability",
    "Cap On Liability",
    "Liquidated Damages",
    "Warranty Duration",
    "Insurance",
    "Covenant Not To Sue",
    "Third Party Beneficiary",
]

# The answer column name (Notice Period has a space before Answer — CUAD typo)
ANSWER_COL_MAP = {
    "Notice Period To Terminate Renewal": "Notice Period To Terminate Renewal- Answer",
}

def get_answer_col(clause_col: str) -> str:
    """Return the answer column name for a clause column."""
    return ANSWER_COL_MAP.get(clause_col, clause_col + "-Answer")

# Risk-focused subset — what your LLM pipeline should detect
RISK_CLAUSE_COLS = [
    "Termination For Convenience",
    "Cap On Liability",
    "Uncapped Liability",
    "Anti-Assignment",
    "Change Of Control",
    "Ip Ownership Assignment",
    "Non-Compete",
    "Exclusivity",
    "Liquidated Damages",
    "Audit Rights",
    "Insurance",
    "Minimum Commitment",
    "Governing Law",
    "Renewal Term",
    "Notice Period To Terminate Renewal",
]