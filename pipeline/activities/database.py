"""
pipeline/activities/database.py
All Postgres read/write operations as Temporal activities.
"""
## This file contains three activities that handle all Postgres read/write operations: inserting a contract, bulk-inserting clauses, and updating a contract's pipeline status.
"""
Why separate database activities instead of writing to Postgres directly inside other activities?
- Two reasons. First, Temporal replays workflow history on retry  if you mix database writes with PDF extraction in the same activity, a retry could double-write data. Separate activities make each operation atomic and independently retryable. 
- Second, it keeps concerns separated pdf.py knows about PDFs, database.py knows about Postgres, neither knows about the other.
"""
import uuid
from dataclasses import dataclass
from typing import Optional

from temporalio import activity

from db.connection import get_pool


# ── Dataclasses ───────────────────────────────────────────────────────────────

@dataclass
class InsertContractInput:
    filename:             str
    title:                str
    contract_type:        str
    source:               str           # 'cuad' | 'user_upload'
    s3_path:              Optional[str]
    full_text:            str
    page_count:           int
    char_count:           int
    quality_score:        float
    extraction_strategy:  str
    quality_warning:      Optional[str]
    tenant_id:            str = "default"


@dataclass
class InsertContractOutput:
    contract_id: str
    already_existed: bool


@dataclass
class InsertClauseInput:
    contract_id:     str
    clause_type:     str
    text:            str
    answer:          Optional[str]
    start_char:      Optional[int]
    confidence:      float
    is_cuad_labeled: bool


@dataclass
class UpdateStatusInput:
    contract_id:   str
    status:        str
    error_message: Optional[str] = None


# ── Activities ────────────────────────────────────────────────────────────────

@activity.defn
async def insert_contract(params: InsertContractInput) -> InsertContractOutput:
    """
    Upsert a contract into Postgres.
    If the filename already exists, update it and return already_existed=True.
    """
    pool = await get_pool()

    async with pool.acquire() as conn:
        existing = await conn.fetchval(
            "SELECT id FROM contracts WHERE filename = $1",
            params.filename,
        )
        
        ## Temporal activities can be retried. If the network drops after the INSERT succeeds but before Temporal receives the result, it will retry the activity. 
        ## Without upsert, the retry would crash on a unique constraint violation because filename is UNIQUE. With upsert, the retry safely updates the existing row instead of crashing.
        if existing:
            await conn.execute("""
                UPDATE contracts SET
                    s3_path             = $2,
                    full_text           = $3,
                    page_count          = $4,
                    quality_score       = $5,
                    extraction_strategy = $6,
                    quality_warning     = $7,
                    status              = 'extracting',
                    updated_at          = now()
                WHERE id = $1
            """,
                existing,
                params.s3_path, params.full_text, params.page_count,
                params.quality_score, params.extraction_strategy,
                params.quality_warning,
            )
            activity.logger.info(f"Updated existing contract: {params.filename}")
            return InsertContractOutput(
                contract_id=str(existing),
                ##  tells the workflow whether this is a fresh ingestion or a re-run useful for logging and debugging.
                already_existed=True,
            )

        ## extracting status: The contract enters the database in extracting state, not ready 
        ## If the worker crashes mid-pipeline, the contract stays in extracting you can query for stuck contracts and re-run them:
        row = await conn.fetchrow("""
            INSERT INTO contracts (
                filename, title, contract_type, source,
                s3_path, full_text, page_count,
                quality_score, extraction_strategy, quality_warning,
                tenant_id, status
            ) VALUES (
                $1, $2, $3, $4,
                $5, $6, $7,
                $8, $9, $10,
                $11, 'extracting'
            )
            RETURNING id
        """,
            params.filename, params.title, params.contract_type, params.source,
            params.s3_path, params.full_text, params.page_count,
            params.quality_score, params.extraction_strategy, params.quality_warning,
            params.tenant_id,
        )

    activity.logger.info(f"Inserted contract: {params.filename} → {row['id']}")
    return InsertContractOutput(contract_id=str(row["id"]), already_existed=False)


@activity.defn
async def insert_clauses_batch(clauses: list[InsertClauseInput]) -> int:
    """
    Bulk-insert a list of clauses for one contract.
    Returns number of clauses inserted.
    """
    if not clauses:
        return 0

    pool = await get_pool()

    ## delete makes the activity idempotent safe to run multiple times. 
    ## If the embedding step fails and the workflow retries from the classification step, you don't end up with duplicate clauses. Clean slate every time.
    async with pool.acquire() as conn:
        # Delete existing clauses of same type for this contract
        # (safe to re-run: idempotent)
        contract_id = uuid.UUID(clauses[0].contract_id)
        is_cuad = clauses[0].is_cuad_labeled
        await conn.execute(
            "DELETE FROM clauses WHERE contract_id = $1 AND is_cuad_labeled = $2",
            contract_id, is_cuad,
        )

        records = [
            (
                uuid.UUID(c.contract_id),
                c.clause_type,
                ## Capping at 2000 characters keeps the database lean and ensures the embedding API doesn't get overloaded.
                c.text[:2000],          # cap at 2000 chars
                c.answer,
                c.start_char,
                c.confidence,
                c.is_cuad_labeled,
            )
            for c in clauses
        ]

        ## executemany sends all records in one round trip to Postgres instead of one query per clause
        ## For a contract with 12 risk clauses, this is 12x fewer network round trips. At scale across 499 contracts, this matters significantly.
        await conn.executemany("""
            INSERT INTO clauses (
                contract_id, clause_type, text, answer,
                start_char, confidence, is_cuad_labeled
            ) VALUES ($1, $2, $3, $4, $5, $6, $7)
        """, records)

    activity.logger.info(
        f"Inserted {len(clauses)} clauses for contract {clauses[0].contract_id}"
    )
    return len(clauses)


@activity.defn
async def update_contract_status(params: UpdateStatusInput) -> None:
    """Update pipeline status on a contract row."""
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute("""
            UPDATE contracts
            SET status        = $2,
                error_message = $3,
                updated_at    = now()
            WHERE id = $1
        """,
        ## asyncpg is strict about types. Postgres expects a UUID type for the id column, not a string. 
            uuid.UUID(params.contract_id),
            params.status,
            params.error_message,
        )