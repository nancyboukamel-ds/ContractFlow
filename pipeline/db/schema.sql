-- pipeline/db/schema.sql
-- Run once to set up the Redline database.
-- Usage: psql -U redline -d redline -f pipeline/db/schema.sql

-- ── Extensions ────────────────────────────────────────────────────────────────
-- vector:  pgvector — adds vector(1536) column type and <=> cosine distance operator
--          required for semantic search / RAG
-- pg_trgm: trigram indexes — powers BM25-style full-text keyword search
--          required for hybrid search (vector + keyword)
CREATE EXTENSION IF NOT EXISTS vector;
CREATE EXTENSION IF NOT EXISTS pg_trgm;

-- ── Table 1: contracts ────────────────────────────────────────────────────────
-- One row per PDF contract.
-- source = 'cuad'        → from the CUAD dataset (499 contracts)
-- source = 'user_upload' → uploaded by a real user through the frontend
-- status tracks pipeline progress:
--   pending → uploading → extracting → classifying → embedding → ready | failed
CREATE TABLE IF NOT EXISTS contracts (
    id                   UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    filename             TEXT UNIQUE NOT NULL,   -- exact PDF filename, used to find it on disk/S3
    title                TEXT NOT NULL,           -- human-readable (filename without extension)
    contract_type        TEXT,                    -- e.g. "Affiliate Agreement", "License Agreement"
    source               TEXT NOT NULL,           -- 'cuad' | 'user_upload'
    tenant_id            TEXT NOT NULL DEFAULT 'default',
    s3_path              TEXT,                    -- where the PDF lives in S3
    full_text            TEXT,                    -- full extracted markdown text
    page_count           INTEGER,
    char_count           INTEGER,
    quality_score        FLOAT,                   -- 0.0-1.0 from quality check activity
    extraction_strategy  TEXT,                    -- 'direct' | 'ocr' | 'mixed'
    quality_warning      TEXT,                    -- human-readable warning if quality is low
    status               TEXT NOT NULL DEFAULT 'pending',
    error_message        TEXT,                    -- populated if status = 'failed'
    created_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at           TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- ── Table 2: clauses ──────────────────────────────────────────────────────────
-- One row per clause per contract.
-- is_cuad_labeled = TRUE  → human-annotated ground truth from the CUAD CSV
--                           confidence = 1.0, used for eval scoring
-- is_cuad_labeled = FALSE → detected by the LLM classification activity
--                           confidence = 0.7-0.9, used for RAG search
-- embedding: 1536-dimensional vector from text-embedding-3-small
--            populated by the embed_contract_clauses activity
CREATE TABLE IF NOT EXISTS clauses (
    id               UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    contract_id      UUID NOT NULL REFERENCES contracts(id) ON DELETE CASCADE,
    clause_type      TEXT NOT NULL,      -- e.g. 'Cap On Liability', 'Anti-Assignment'
    text             TEXT NOT NULL,      -- extracted clause text (max 2000 chars)
    answer           TEXT,               -- 'Yes' | 'No' | date | entity name
    start_char       INTEGER,            -- position in full_text (from CUAD annotations)
    end_char         INTEGER,
    page_number      INTEGER,
    confidence       FLOAT NOT NULL DEFAULT 1.0,
    is_cuad_labeled  BOOLEAN NOT NULL DEFAULT FALSE,
    embedding        vector(1536),       -- pgvector column — populated after extraction
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- ── Table 3: risk_reports ─────────────────────────────────────────────────────
-- Output of ContractReviewWorkflow (your existing ai-contract-review app).
-- completion_reason distinguishes between:
--   'approved'              → human reviewer approved the report
--   'timeout'              → 3-day wait expired with no decision
--   'max_revisions_reached' → hit max_revisions limit without approval
CREATE TABLE IF NOT EXISTS risk_reports (
    id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    contract_id         UUID REFERENCES contracts(id) ON DELETE CASCADE,
    workflow_id         TEXT,
    overall_risk_level  TEXT,            -- 'High' | 'Medium' | 'Low'
    risk_justification  TEXT,
    report_json         JSONB,           -- full structured report as JSON
    prompt_version      TEXT,            -- which prompt version generated this
    model               TEXT,            -- which LLM model was used
    approved_by         TEXT,            -- reviewer name from assign_reviewer signal
    revision_count      INTEGER NOT NULL DEFAULT 0,
    completion_reason   TEXT,            -- 'approved' | 'timeout' | 'max_revisions_reached'
    latency_ms          INTEGER,         -- total workflow duration
    cost_usd            FLOAT,           -- estimated LLM cost
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- ── Table 4: qa_interactions ──────────────────────────────────────────────────
-- Contract Q&A history — user asks a question, system answers with citations.
-- citations stored as JSONB: [{clause_type, excerpt, page_number}]
-- This lets the frontend show exactly which clause text backed up each answer.
CREATE TABLE IF NOT EXISTS qa_interactions (
    id           UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    contract_id  UUID REFERENCES contracts(id) ON DELETE CASCADE,
    workflow_id  TEXT,
    question     TEXT NOT NULL,
    answer       TEXT,
    citations    JSONB,           -- [{clause_type, excerpt, page_number}]
    confidence   FLOAT,
    latency_ms   INTEGER,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- ── Table 5: eval_runs ────────────────────────────────────────────────────────
-- One row per eval run — stores aggregate scores across all tested contracts.
-- Two tables instead of one because you want to query at two levels:
--   "what was the overall recall for prompt v2?"        → eval_runs
--   "which contracts did prompt v2 specifically fail on?" → eval_contract_results
-- passed = TRUE means avg_recall >= 0.60 threshold
CREATE TABLE IF NOT EXISTS eval_runs (
    id                 UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    run_id             TEXT UNIQUE NOT NULL,
    prompt_version     TEXT NOT NULL,
    model              TEXT NOT NULL,
    avg_recall         FLOAT,            -- recall across all tested contracts
    avg_precision      FLOAT,
    hallucination_rate FLOAT,            -- fraction of extracted risks not in ground truth
    contracts_tested   INTEGER,
    passed             BOOLEAN,          -- TRUE if avg_recall >= threshold (0.60)
    created_at         TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- ── Table 6: eval_contract_results ───────────────────────────────────────────
-- One row per contract per eval run — per-contract scores and clause-level detail.
-- clause_results JSONB stores per-clause judge output:
--   [{clause_type, captured, confidence, reasoning}]
CREATE TABLE IF NOT EXISTS eval_contract_results (
    id               UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    eval_run_id      UUID REFERENCES eval_runs(id) ON DELETE CASCADE,
    contract_id      UUID REFERENCES contracts(id),
    recall           FLOAT,
    precision        FLOAT,
    num_known_risks  INTEGER,   -- how many CUAD ground truth clauses exist
    num_captured     INTEGER,   -- how many your pipeline detected correctly
    num_hallucinated INTEGER,   -- how many your pipeline invented (not in ground truth)
    clause_results   JSONB,     -- per-clause judge output
    latency_ms       INTEGER
);

-- ── Indexes ───────────────────────────────────────────────────────────────────

-- VECTOR SIMILARITY INDEX (IVFFlat)
-- Used by: search_clauses activity for semantic RAG search
-- IVFFlat groups vectors into 'lists' clusters and only searches nearest clusters.
-- lists=100 is appropriate for ~7,500 vectors (rule of thumb: sqrt(num_vectors))
-- Much faster than exact scan at the cost of slight recall loss (~5%)
-- Required before semantic search will work efficiently.
CREATE INDEX IF NOT EXISTS clauses_embedding_idx
    ON clauses USING ivfflat (embedding vector_cosine_ops)
    WITH (lists = 100);

-- FULL-TEXT SEARCH INDEXES (GIN on tsvector)
-- Used by: search_clauses activity for BM25 keyword search (hybrid RAG)
-- GIN (Generalized Inverted Index) pre-builds a token → row mapping.
-- to_tsvector('english', ...) stems words: "liability" → "liabil",
--   "termination" → "terminat", removes stop words ("the", "of", "in")
-- @@ operator checks if a document matches a query — uses these indexes.
-- Without them, every search would scan every row (sequential scan).
CREATE INDEX IF NOT EXISTS contracts_fts_idx
    ON contracts USING GIN (to_tsvector('english', COALESCE(full_text, '')));

CREATE INDEX IF NOT EXISTS clauses_fts_idx
    ON clauses USING GIN (to_tsvector('english', text));

-- BTREE INDEXES FOR FAST LOOKUPS
-- contracts_status_idx: "show me all failed contracts" → instant
-- contracts_tenant_idx: multi-tenant isolation → filters by tenant before other ops
-- contracts_filename_idx: find_pdf() lookup and upsert check → instant
-- clauses_contract_idx: "get all clauses for this contract" → instant
-- clauses_type_idx: "get all Cap On Liability clauses" → instant
-- clauses_cuad_idx: "get all human-labeled clauses for eval" → instant
CREATE INDEX IF NOT EXISTS contracts_source_idx   ON contracts(source);
CREATE INDEX IF NOT EXISTS contracts_status_idx   ON contracts(status);
CREATE INDEX IF NOT EXISTS contracts_tenant_idx   ON contracts(tenant_id);
CREATE INDEX IF NOT EXISTS contracts_filename_idx ON contracts(filename);
CREATE INDEX IF NOT EXISTS clauses_contract_idx   ON clauses(contract_id);
CREATE INDEX IF NOT EXISTS clauses_type_idx       ON clauses(clause_type);
CREATE INDEX IF NOT EXISTS clauses_cuad_idx       ON clauses(is_cuad_labeled);