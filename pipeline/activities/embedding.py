"""
pipeline/activities/embedding.py
Embed clause text into pgvector using text-embedding-3-small.
Also supports hybrid search (vector + BM25 full-text).

This file contains two activities: 
one that converts clause text into vectors and stores them in pgvector,
and one that searches across all stored vectors using hybrid search (vector similarity + BM25 keyword matching).

Why embeddings?
Text search with keywords breaks on legal language because the same clause can be written dozens of ways.
 Embeddings convert text to numbers that capture meaning semantically similar clauses end up close together in vector space, even with completely different words.
"""
import os
import uuid
from dataclasses import dataclass

from openai import OpenAI
from temporalio import activity

from config import EMBEDDING_MODEL
from db.connection import get_pool


# ── Dataclasses ───────────────────────────────────────────────────────────────

@dataclass
class EmbedClausesInput:
    contract_id: str
    batch_size:  int = 50    # clauses per API call


@dataclass
class EmbedClausesOutput:
    contract_id:    str
    clauses_embedded: int


@dataclass
class SearchInput:
    query:      str
    top_k:      int  = 5
    tenant_id:  str  = "default"
    use_hybrid: bool = True     # combine vector + BM25


@dataclass
class SearchResult:
    contract_id:   str
    filename:      str
    contract_type: str
    clause_type:   str
    excerpt:       str
    score:         float
    is_cuad_labeled: bool


# ── Activity 1: Embed ─────────────────────────────────────────────────────────

@activity.defn
async def embed_contract_clauses(params: EmbedClausesInput) -> EmbedClausesOutput:
    """
    Fetch all clauses for a contract that have no embedding yet,
    call the OpenAI embedding API in batches,
    and write the vectors back into pgvector.
    """
    pool = await get_pool()

    client = OpenAI(
        api_key=os.environ["OPENROUTER_API_KEY"],
        base_url="https://openrouter.ai/api/v1",
    )

    # Fetch clauses that still need embeddings
    ## Why embedding IS NULL?
    ## Makes the activity idempotent. If the activity fails halfway through and retries,
    # it skips clauses that already have embeddings and only processes the remaining ones.
    # No wasted API calls, no duplicate embeddings.
    async with pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT id, text
            FROM   clauses
            WHERE  contract_id = $1
              AND  embedding IS NULL
        """, uuid.UUID(params.contract_id))

    if not rows:
        activity.logger.info(
            f"No clauses need embedding for {params.contract_id}"
        )
        return EmbedClausesOutput(
            contract_id=params.contract_id,
            clauses_embedded=0,
        )

    activity.logger.info(
        f"Embedding {len(rows)} clauses for {params.contract_id}"
    )
    embedded = 0

    for i in range(0, len(rows), params.batch_size):
        batch = rows[i : i + params.batch_size]
        texts = [row["text"] for row in batch]

        ## text-embedding-3-small produces 1536-dimensional vectors. text-embedding-3-large produces 3072-dimensional vectors. For legal clause similarity, small is accurate enough and costs 5x less.
        response = client.embeddings.create(
            model=EMBEDDING_MODEL, # "text-embedding-3-small"
            input=texts,
        )
        
        ## Why update in the same loop instead of collecting all embeddings then updating?
        ## Memory. If you have 500 clauses × 1536 floats × 4 bytes = ~3MB of vectors in memory before writing, that's fine. But at scale with thousands of contracts it becomes a problem. Writing each batch immediately keeps memory flat.
        async with pool.acquire() as conn:
            for j, row in enumerate(batch):
                vector = response.data[j].embedding
                await conn.execute("""
                    UPDATE clauses SET embedding = $1::vector WHERE id = $2
                """, str(vector), row["id"])

        embedded += len(batch)
        activity.heartbeat({
            "stage":    "embedding",
            "embedded": embedded,
            "total":    len(rows),
            "pct":      round(embedded / len(rows) * 100),
        })

    return EmbedClausesOutput(
        contract_id=params.contract_id,
        clauses_embedded=embedded,
    )


# ── Activity 2: Hybrid search ─────────────────────────────────────────────────

@activity.defn
async def search_clauses(params: SearchInput) -> list[SearchResult]:
    """
    Hybrid search: vector similarity + BM25 full-text, merged and re-ranked.

    Vector search  — finds semantically similar clauses (catches paraphrases)
    BM25 search    — finds exact legal terms (catches precise citations)
    Hybrid result  — union of both, scored by (vector_rank + bm25_rank) / 2
    """
    pool = await get_pool()

    client = OpenAI(
        api_key=os.environ["OPENROUTER_API_KEY"],
        base_url="https://openrouter.ai/api/v1",
    )

    # Embed the query
    # The user's question or search query gets converted to the same 1536-dimensional vector space as the stored clause embeddings. 
    # Distance in this space = semantic similarity.
    ## 
    q_resp = client.embeddings.create(
        model=EMBEDDING_MODEL,
        input=[params.query],
    )
    query_vector = q_resp.data[0].embedding

    async with pool.acquire() as conn:
 
        ## semntic search
        ## <=> is pgvector's cosine distance operator. 1 - distance converts distance to similarity 1.0 means identical, 0.0 means completely unrelated. 
        ## ROW_NUMBER() assigns rank 1 to the most similar clause, rank 2 to the second most similar, etc.
        ## Why rank instead of just using the score?
        ## Because BM25 scores and vector scores are on completely different scales you can't add them. But ranks are comparable
        ## If you want top_k=5 final results, fetch 5 * 3 = 15 candidates from each search. Fetching 3× gives the merge enough candidates to confidently pick the best 5 final results. 

        ## BM25 search
        ## to_tsvector converts clause text to a searchable token list. 
        ## Input:  "The liability of either party shall not exceed the fees paid"
        ## Output: 'either':4 'exceed':8 'fee':10 'liabil':2 'paid':11 'parti':5 'shall':6
        ##  "liability" → "liabil", "parties" → "parti", "fees" → "fee". This is stemming reduces words to their root so "liabilities", "liable", "liability" all match. "The", "of", "not" are removed as stop words.

        ## plainto_tsquery converts the query to a boolean search expression.
        ## Input:  "unlimited liability no termination rights"
        ## Output: 'unlimit' & 'liabil' & 'terminat' & 'right'
        ## The & means ALL terms must appear. plainto_tsquery handles this automatically
        ## @@ checks if they match. ts_rank scores how well they match more occurrences of query terms = higher rank.
        
        ## Reciprocal Rank Fusion (RRF): 1.0 / (60 + v.vector_rank) + COALESCE(1.0 / (60 + b.bm25_rank), 0)
        ## The 60 is the smoothing constant from the original RRF paper. It prevents the top-ranked result from dominating too heavily:
        ## left join instead of inner join: If a clause scores well on vector similarity but has no BM25 match, it still appears in the final results with just its vector contribution. An INNER JOIN would discard it entirely, losing good semantic matches that don't contain the exact search terms.
        if params.use_hybrid:
            # ── Hybrid: vector + BM25 ──────────────────────────────────────
            rows = await conn.fetch("""
                WITH vector_ranked AS (
                    SELECT
                        cl.id,
                        cl.contract_id,
                        cl.clause_type,
                        cl.text,
                        cl.is_cuad_labeled,
                        co.filename,
                        co.contract_type,
                        1 - (cl.embedding <=> $1::vector) AS vector_score,
                        ROW_NUMBER() OVER (
                            ORDER BY cl.embedding <=> $1::vector
                        ) AS vector_rank
                    FROM clauses cl
                    JOIN contracts co ON co.id = cl.contract_id
                    WHERE cl.embedding IS NOT NULL
                      AND co.tenant_id = $3
                    LIMIT $2 * 3
                ),
                bm25_ranked AS (
                    SELECT
                        cl.id,
                        ROW_NUMBER() OVER (
                            ORDER BY ts_rank(
                                to_tsvector('english', cl.text),
                                plainto_tsquery('english', $4)
                            ) DESC
                        ) AS bm25_rank
                    FROM clauses cl
                    JOIN contracts co ON co.id = cl.contract_id
                    WHERE to_tsvector('english', cl.text)
                          @@ plainto_tsquery('english', $4)
                      AND co.tenant_id = $3
                    LIMIT $2 * 3
                )
                SELECT
                    v.id,
                    v.contract_id::text,
                    v.filename,
                    v.contract_type,
                    v.clause_type,
                    v.text        AS excerpt,
                    v.is_cuad_labeled,
                    -- Reciprocal rank fusion score
                    (1.0 / (60 + v.vector_rank) +
                     COALESCE(1.0 / (60 + b.bm25_rank), 0)) AS score
                FROM vector_ranked v
                LEFT JOIN bm25_ranked b ON b.id = v.id
                ORDER BY score DESC
                LIMIT $2
            """,
                query_vector, params.top_k,
                params.tenant_id, params.query,
            )
        else:
            # ── Vector only ────────────────────────────────────────────────
            rows = await conn.fetch("""
                SELECT
                    cl.id,
                    cl.contract_id::text,
                    co.filename,
                    co.contract_type,
                    cl.clause_type,
                    cl.text        AS excerpt,
                    cl.is_cuad_labeled,
                    1 - (cl.embedding <=> $1::vector) AS score
                FROM clauses cl
                JOIN contracts co ON co.id = cl.contract_id
                WHERE cl.embedding IS NOT NULL
                  AND co.tenant_id = $3
                ORDER BY cl.embedding <=> $1::vector
                LIMIT $2
            """,
                query_vector, params.top_k, params.tenant_id,
            )

    return [
        SearchResult(
            contract_id    = row["contract_id"],
            filename       = row["filename"],
            contract_type  = row["contract_type"],
            clause_type    = row["clause_type"],
            excerpt        = row["excerpt"][:400],
            score          = float(row["score"]),
            is_cuad_labeled= row["is_cuad_labeled"],
        )
        for row in rows
    ]