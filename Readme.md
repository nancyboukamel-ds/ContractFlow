# Redline AI Legal Contract Review Platform

> **Durable, human-in-the-loop contract risk analysis powered by Temporal, RAG, and LLMs**

Redline is a production-grade AI platform that ingests legal contracts (PDF), extracts and classifies risk clauses using LLMs, stores semantic embeddings for RAG-based search, and orchestrates a human-in-the-loop approval workflow all on top of Temporal's durable execution engine.

Built against the **CUAD dataset** (510 real commercial contracts, 13,000+ expert annotations across 41 clause categories) for ground-truth evaluation.

---

## Table of Contents

- [Architecture](#architecture)
- [What It Does](#what-it-does)
- [Tech Stack](#tech-stack)
- [Project Structure](#project-structure)
- [Key Design Decisions](#key-design-decisions)
- [Data Pipeline](#data-pipeline)
- [AI Contract Review App](#ai-contract-review-app)
- [Evaluation Framework](#evaluation-framework)
- [Getting Started](#getting-started)
- [Running the Pipeline](#running-the-pipeline)
- [Running the Contract Review App](#running-the-contract-review-app)
- [API Reference](#api-reference)
- [Database Schema](#database-schema)
- [Workflow Screenshots](#workflow-screenshots)

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        Client Layer                              │
│         FastAPI REST API  ·  curl  ·  Future: Next.js UI        │
└────────────────────────────┬────────────────────────────────────┘
                             │ REST + WebSocket
┌────────────────────────────▼────────────────────────────────────┐
│                     Temporal Server                              │
│              Durable workflow orchestration engine               │
│         localhost:7233  ·  UI: localhost:8080                    │
└──────┬──────────────────────────────────┬───────────────────────┘
       │                                  │
┌──────▼──────────┐              ┌────────▼────────────┐
│  Pipeline Worker │              │  Review Worker       │
│  pipeline-queue  │              │  contract-review-    │
│                  │              │  queue               │
│  Workflows:      │              │                      │
│  · Ingest        │              │  Workflows:          │
│  · BatchIngest   │              │  · ContractReview    │
│                  │              │  · PDFSummary        │
│  Activities:     │              │                      │
│  · PDF extract   │              │  Activities:         │
│  · S3 upload     │              │  · extract_pdf       │
│  · LLM classify  │              │  · call_llm          │
│  · Embed clauses │              │                      │
│  · DB insert     │              │                      │
└──────┬───────────┘              └────────┬─────────────┘
       │                                   │
┌──────▼───────────────────────────────────▼─────────────────────┐
│                     Infrastructure                               │
│                                                                  │
│   PostgreSQL 16 + pgvector    iDrive E2 (S3-compatible)         │
│   ─────────────────────────   ─────────────────────────         │
│   · contracts table           · Raw PDF storage                  │
│   · clauses + embeddings      · s3://bucket/cuad/               │
│   · risk_reports              · s3://bucket/uploads/            │
│   · eval_runs                                                    │
│   · qa_interactions                                              │
│                                                                  │
│   OpenRouter (LLM proxy)      Temporal Docker Compose           │
│   ─────────────────────       ──────────────────────            │
│   · gpt-4o-mini               · temporal server                 │
│   · text-embedding-3-small    · temporal-postgresql             │
│                               · temporal-ui                      │
└────────────────────────────────────────────────────────────────┘
```

---

## What It Does

### 1. Data Pipeline (`/pipeline`)

Ingests the entire CUAD dataset (510 commercial contracts) into a searchable knowledge base:

```
PDF on disk
    │
    ▼
Upload to S3
    │
    ▼
PDF Quality Assessment
(direct / OCR / mixed strategy)
    │
    ▼
Text Extraction (pymupdf4llm → markdown)
    │
    ▼
Store contract in PostgreSQL
    │
    ├──▶ Insert CUAD ground-truth clauses (human-labeled, confidence=1.0)
    │
    ├──▶ LLM clause classification (detects additional clause types)
    │
    └──▶ Embed all clauses → pgvector (text-embedding-3-small, 1536 dims)
```

### 2. AI Contract Review (`/apps/ai-contract-review`)

End-to-end contract review workflow with human-in-the-loop approval:

```
User submits PDF S3 paths
    │
    ▼
Fan-out: one PDFSummaryWorkflow per PDF (parallel)
    │
    ├──▶ extract_pdf activity (download, OCR if needed, markdown)
    └──▶ call_llm activity (summarize + extract risks per contract)
    │
    ▼
Fan-in: collect all summaries
    │
    ▼
Synthesis: call_llm (cross-contract risk report)
    │
    ▼
HITL Pause (up to 3 days)
    │
    ├──▶ Query:  GET /contract-review/{id}/report
    ├──▶ Signal: POST /contract-review/{id}/assign  (assign reviewer)
    ├──▶ Update: POST /contract-review/{id}/approve (approve report)
    └──▶ Update: POST /contract-review/{id}/revise  (request revision)
    │
    ▼
Complete: return approved risk report
```

### 3. Hybrid RAG Search

Semantic + keyword search across the entire contract knowledge base:

```
User query: "uncapped liability with no termination rights"
    │
    ├──▶ Vector search:  embed query → cosine similarity in pgvector
    ├──▶ BM25 search:    tokenize query → full-text GIN index
    │
    └──▶ Reciprocal Rank Fusion → merged, reranked results
```

---

## Tech Stack

| Layer | Technology | Why |
|---|---|---|
| Workflow orchestration | **Temporal** | Durable execution — survives crashes, automatic retries, HITL waits |
| API | **FastAPI** | Async, typed, auto-docs |
| Database | **PostgreSQL 16** | Structured data + pgvector embeddings + BM25 full-text search in one DB |
| Vector search | **pgvector** | No separate vector DB needed — hybrid search in pure SQL |
| PDF extraction | **pymupdf4llm** | LLM-optimized markdown output — preserves tables and structure |
| LLM | **OpenRouter → gpt-4o-mini** | Cheap, fast, good enough for clause extraction; swappable |
| Embeddings | **text-embedding-3-small** | 1536 dims, low cost, high quality for legal text |
| Storage | **iDrive E2** (S3-compatible) | S3 API, cheaper than AWS for large PDF storage |
| Dataset | **CUAD v1** | 510 real contracts, 13,000+ expert annotations, CC BY 4.0 |

---

## Project Structure

```
durable-pdf-pipeline/
├── apps/
│   ├── ai-contract-review/          # Contract review app
│   │   ├── activities.py            # extract_pdf, call_llm
│   │   ├── child_workflow.py        # PDFSummaryWorkflow (per-PDF)
│   │   ├── parent_workflow.py       # ContractReviewWorkflow (HITL)
│   │   ├── schemas.py               # Pydantic output schemas
│   │   ├── prompts.py               # LLM prompt templates
│   │   └── worker.py                # Worker entry point
│   │
│   └── client-app/                  # FastAPI REST API
│       └── main.py                  # All routes
│
├── pipeline/                        # CUAD ingestion pipeline
│   ├── activities/
│   │   ├── pdf.py                   # Quality check + extraction (direct/OCR/mixed)
│   │   ├── storage.py               # S3 upload/download
│   │   ├── database.py              # Postgres insert/update
│   │   ├── classification.py        # LLM clause detection
│   │   └── embedding.py             # pgvector embed + hybrid search
│   ├── workflows/
│   │   └── ingestion.py             # IngestContractWorkflow + BatchIngestion
│   ├── db/
│   │   └── schema.sql               # Full DB schema with indexes
│   ├── config.py                    # All configuration + CUAD column names
│   ├── run_ingestion.py             # CLI entry point
│   └── worker.py                    # Pipeline worker
│
├── CUAD_v1/                         # Downloaded separately (not in git)
│   ├── master_clauses.csv           # 510 contracts × 41 clause annotations
│   └── full_contract_pdf/           # 510 PDFs organized by type
│       ├── Part_I/
│       ├── Part_II/
│       └── Part_III/
│
└── setup/
    └── services/
        ├── temporal.service         # systemd service for Temporal
        └── temporal-worker.service  # systemd service for worker
```

---

## Key Design Decisions

### Why Temporal?

Without Temporal, this system would require:
- A job queue (Redis/RabbitMQ) for async processing
- A database to track pipeline state
- Cron jobs for retry logic
- Polling loops for the 3-day HITL wait
- Manual crash recovery

With Temporal, all of this is built in. The HITL pause is just `await workflow.wait_condition(...)` — the workflow is suspended in Temporal's persisted state and wakes up when a signal arrives, consuming zero resources while waiting.

### Why pgvector instead of a dedicated vector DB?

Three capabilities are needed simultaneously:
1. Structured queries (`WHERE status = 'ready' AND source = 'cuad'`)
2. Vector similarity search (semantic RAG)
3. Full-text keyword search (BM25)

pgvector gives all three in one database. Pinecone can't do structured queries. SQLite can't do vectors. PostgreSQL does everything.

### Why hybrid RAG (vector + BM25)?

Legal language breaks pure vector search because:
- "liability uncapped" and "exposure has no limit" are semantically identical but share no keywords
- Exact legal citations ("Section 11.4(b)") and party names are blurred by embeddings

Hybrid search uses **Reciprocal Rank Fusion** to merge both rankings:
```
score = 1/(60 + vector_rank) + 1/(60 + bm25_rank)
```
The constant 60 prevents top-ranked results from dominating — democratic merging.

### Why two clause types in the same table?

```sql
is_cuad_labeled = TRUE   -- human ground truth, confidence=1.0
                         -- used for eval scoring
is_cuad_labeled = FALSE  -- LLM detected, confidence=0.7-0.9
                         -- used for user-uploaded contracts
```

One table, one search query, two trust levels. The eval framework uses `WHERE is_cuad_labeled = TRUE` for ground truth.

### Why `Type=oneshot` for the Temporal systemd service?

The Temporal Docker Compose runs `docker compose up -d` which starts containers in detached mode and exits immediately. `Type=oneshot` with `RemainAfterExit=yes` tells systemd that the service is "active" even after the process exits — because the containers keep running independently.

---

## Data Pipeline

### CUAD Dataset

The **Contract Understanding Atticus Dataset (CUAD) v1** is a corpus of 510 commercial legal contracts manually labeled by lawyers to identify 41 categories of important clauses.

- **License**: CC BY 4.0 (free for commercial and non-commercial use)
- **Contracts**: 510 real SEC filings across 25 contract types
- **Annotations**: 13,000+ expert labels across 41 clause categories
- **Source**: EDGAR (SEC public filings)

Key risk clause types tracked by this pipeline:

| Clause Type | Prevalence | Risk |
|---|---|---|
| Anti-Assignment | 73% | Can't transfer contract without permission |
| Cap On Liability | 54% | Maximum financial exposure |
| Audit Rights | 42% | Other party can inspect your books |
| Termination For Convenience | 36% | Can exit without cause |
| Exclusivity | 35% | Locked into one vendor/partner |
| Minimum Commitment | 33% | Must buy minimum amount |
| Insurance | 33% | Must maintain insurance coverage |
| Uncapped Liability | 22% | No limit on financial exposure — dangerous |
| Liquidated Damages | 12% | Fixed penalty on breach |

### PDF Quality Assessment

Before extraction, each PDF is assessed and routed to the appropriate strategy:

```
avg_chars_per_page < 100     → OCR (scanned PDF)
non_ascii_ratio > 30%        → OCR (encoding issue)
text_page_coverage < 50%     → Mixed (some pages scanned)
otherwise                    → Direct (clean native text)
```

In the CUAD dataset, all 500 matched PDFs use `direct` extraction with `quality_score=1.0`.

### Ingestion Workflow

```
CUADBatchIngestionWorkflow (parent)
    │
    ├── IngestContractWorkflow × N (children, parallel batches)
    │       │
    │       ├── upload_pdf_to_s3          (S3 activity)
    │       ├── assess_pdf_quality        (PDF activity)
    │       ├── extract_pdf_text          (PDF activity)
    │       ├── insert_contract           (DB activity)
    │       ├── insert_clauses_batch      (DB activity, CUAD labels)
    │       ├── classify_clauses          (LLM activity)
    │       ├── insert_clauses_batch      (DB activity, LLM detected)
    │       └── embed_contract_clauses    (Embedding activity)
    │
    └── Progress queryable via get_progress() at any time
```


## Evaluation Framework

The pipeline stores two types of clauses per contract:

- `is_cuad_labeled=TRUE` — 13,000+ human-annotated ground truth labels
- `is_cuad_labeled=FALSE` — LLM-detected clauses

Eval measures: how many CUAD ground truth clauses did the LLM correctly detect?

```sql
-- Ground truth for eval
SELECT clause_type, text
FROM clauses
WHERE contract_id = $1
AND is_cuad_labeled = TRUE;

-- LLM detections to score
SELECT clause_type, text, confidence
FROM clauses
WHERE contract_id = $1
AND is_cuad_labeled = FALSE;
```

Results are stored in `eval_runs` and `eval_contract_results` tables for trend tracking across prompt versions.

---

## Getting Started

### Prerequisites

- Python 3.11+
- Docker + Docker Compose
- PostgreSQL 16 with pgvector extension
- iDrive E2 account (or any S3-compatible storage)
- OpenRouter API key

### 1. Clone and set up the environment

```bash
git clone https://github.com/nancyboukamel-ds/redline.git
cd redline
python -m venv .venv
source .venv/bin/activate
pip install -r apps/ai-contract-review/requirements.txt
pip install -r apps/client-app/requirements.txt
pip install -r pipeline/requirements.txt
```

### 2. Start Temporal

```bash
cd setup/samples-server/compose
docker compose -f docker-compose-postgres.yml up -d
```

Or as a systemd service (auto-starts on boot):

```bash
sudo cp setup/services/temporal.service /etc/systemd/system/
sudo systemctl enable --now temporal.service
```

Temporal UI: http://localhost:8080

### 3. Set up the database

The Temporal Docker Compose includes a PostgreSQL container. Set up the Redline database inside it:

```bash
docker exec -it temporal-postgresql psql -U temporal -c "CREATE USER redline WITH PASSWORD 'redline123';"
docker exec -it temporal-postgresql psql -U temporal -c "CREATE DATABASE redline OWNER redline;"
docker exec -it temporal-postgresql psql -U temporal -d redline -c "CREATE EXTENSION IF NOT EXISTS vector;"
docker exec -it temporal-postgresql psql -U temporal -d redline -c "CREATE EXTENSION IF NOT EXISTS pg_trgm;"
docker cp pipeline/db/schema.sql temporal-postgresql:/tmp/schema.sql
docker exec -it temporal-postgresql psql -U redline -d redline -f /tmp/schema.sql
```

### 4. Configure environment variables

```bash
# Pipeline
cp pipeline/.env.example pipeline/.env
nano pipeline/.env

# AI Contract Review app
cp apps/ai-contract-review/.env.example apps/ai-contract-review/.env
nano apps/ai-contract-review/.env

# Client API
cp apps/client-app/.env.example apps/client-app/.env
nano apps/client-app/.env
```

Required variables:

```env
AWS_ACCESS_KEY_ID=your_key
AWS_SECRET_ACCESS_KEY=your_secret
AWS_REGION=us-west-2
AWS_S3_ENDPOINT_URL=https://s3.us-west-2.idrivee2.com
S3_BUCKET=your-bucket
OPENROUTER_API_KEY=your_key
DATABASE_URL=postgresql://redline:redline123@localhost:5432/redline
TEMPORAL_HOST=localhost:7233
TEMPORAL_NAMESPACE=default
CUAD_ROOT=/path/to/CUAD_v1
```

---

## Running the Pipeline

### Dry run (no Temporal, just validates CSV + PDF matching)

```bash
cd pipeline
python run_ingestion.py --dry-run
```

Expected output:
```
Contracts found : 500
PDFs missing    : 10  (special characters in filenames)

Risk clause distribution (ground truth):
  Anti-Assignment     367 contracts (73%) | 596 excerpts
  Cap On Liability    268 contracts (54%) | 617 excerpts
  ...
```

### Test with 1 contract

```bash
# Terminal 1 — start worker
python worker.py

# Terminal 2 — ingest 1 contract
python run_ingestion.py --limit 1 --concurrency 1
```

### Verify results

```bash
docker exec -it temporal-postgresql psql -U redline -d redline -c "
SELECT contract_type, status, page_count, quality_score
FROM contracts;"

docker exec -it temporal-postgresql psql -U redline -d redline -c "
SELECT clause_type, is_cuad_labeled, confidence, left(text, 100)
FROM clauses ORDER BY is_cuad_labeled DESC, clause_type;"
```

### Full ingestion (500 contracts, ~3-4 hours)

```bash
python run_ingestion.py --concurrency 5
```

Watch progress in the Temporal UI at http://localhost:8080 or poll via:
```bash
# Ctrl+C to detach from the polling loop — workflow keeps running
```

---

## Running the Contract Review App

### Start the workers

```bash
# Terminal 1 — AI review worker
cd apps/ai-contract-review
python worker.py

# Terminal 2 — FastAPI client
cd apps/client-app
uvicorn main:app --reload --port 5000
```

### Submit a contract review

```bash
# Start a review (returns immediately with workflow_id)
curl -X POST http://localhost:5000/contract-review/start \
  -H "Content-Type: application/json" \
  -d '{
    "s3_paths": [
      "s3://your-bucket/files/contract1.pdf",
      "s3://your-bucket/files/contract2.pdf"
    ],
    "max_revisions": 2
  }'

# Poll status
curl http://localhost:5000/contract-review/{workflow_id}/status

# Read the full report (when status = awaiting-review)
curl http://localhost:5000/contract-review/{workflow_id}/report

# Assign a reviewer
curl -X POST http://localhost:5000/contract-review/{workflow_id}/assign \
  -H "Content-Type: application/json" \
  -d '{"name": "Nancy"}'

# Approve
curl http://localhost:5000/contract-review/{workflow_id}/approve

# Or request revision
curl -X POST http://localhost:5000/contract-review/{workflow_id}/revise \
  -H "Content-Type: application/json" \
  -d '{"feedback": "Please include more detail on the liability clauses."}'
```

---

## API Reference

| Method | Endpoint | Description |
|---|---|---|
| GET | `/health` | Health check |
| POST | `/process-pdf/execute` | Extract PDF and wait for result |
| POST | `/process-pdf/start` | Start PDF extraction (async) |
| GET | `/workflow/status/{id}` | Get workflow status and result |
| POST | `/contract-review/start` | Start multi-PDF contract review |
| GET | `/contract-review/{id}/status` | Get review status + progress |
| GET | `/contract-review/{id}/report` | Get full risk report |
| POST | `/contract-review/{id}/assign` | Assign a reviewer (Signal) |
| POST | `/contract-review/{id}/revise` | Request revision with feedback (Update) |
| GET | `/contract-review/{id}/approve` | Approve the report (Update) |

---

## Database Schema

```
contracts          — one row per PDF contract
clauses            — one row per clause per contract (with pgvector embedding)
risk_reports       — output of ContractReviewWorkflow
qa_interactions    — contract Q&A history
eval_runs          — aggregate eval scores per prompt version
eval_contract_results — per-contract eval scores
```

### Key indexes

```sql
-- Vector similarity (IVFFlat, lists=100)
-- Powers semantic RAG search
CREATE INDEX clauses_embedding_idx
    ON clauses USING ivfflat (embedding vector_cosine_ops);

-- Full-text search (GIN on tsvector)
-- Powers BM25 keyword search
CREATE INDEX clauses_fts_idx
    ON clauses USING GIN (to_tsvector('english', text));
```

---

## Workflow Screenshots

### 1. Temporal Workers — both workers running

`[INSERT: screenshot of Workers page showing pipeline-queue and contract-review-queue workers]`

### 2. Batch Ingestion Workflow

`[INSERT: screenshot of CUADBatchIngestionWorkflow showing completed child workflows]`

### 3. Single Contract Ingestion — Event History

`[INSERT: screenshot of IngestContractWorkflow event history showing all 7 activity steps]`

### 4. Contract Review Workflow — HITL in action

`[INSERT: screenshot of ContractReviewWorkflow showing awaiting-review status]`

### 5. Contract Review — Approved

`[INSERT: screenshot of ContractReviewWorkflow completed with approval]`

---


### CUAD Clause Distribution (500 contracts)

```
Anti-Assignment             367 contracts (73%) | 596 excerpts
Cap On Liability            268 contracts (54%) | 617 excerpts
Audit Rights                209 contracts (42%) | 599 excerpts
Termination For Convenience 179 contracts (36%) | 217 excerpts
Exclusivity                 174 contracts (35%) | 375 excerpts
Minimum Commitment          164 contracts (33%) | 391 excerpts
Insurance                   159 contracts (32%) | 514 excerpts
Ip Ownership Assignment     121 contracts (24%) | 286 excerpts
Change Of Control           115 contracts (23%) | 200 excerpts
Non-Compete                 113 contracts (23%) | 215 excerpts
Uncapped Liability          109 contracts (22%) | 148 excerpts
Liquidated Damages           60 contracts (12%) | 110 excerpts
```

## License

This project is for educational and portfolio purposes.

The CUAD dataset is licensed under [CC BY 4.0](https://creativecommons.org/licenses/by/4.0/) by The Atticus Project, Inc.

```
@article{hendrycks2021cuad,
  title={CUAD: An Expert-Annotated NLP Dataset for Legal Contract Review},
  author={Dan Hendrycks and Collin Burns and Anya Chen and Spencer Ball},
  journal={arXiv preprint arXiv:2103.06268},
  year={2021}
}
```

---

## Author

**Nancy Boukamel** — AI Engineer
- GitHub: [nancyboukamel-ds](https://github.com/nancyboukamel-ds)
