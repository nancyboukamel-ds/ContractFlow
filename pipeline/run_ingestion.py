"""
pipeline/run_ingestion.py

CLI to:
  1. Read master_clauses.csv
  2. Match each row to its PDF on disk
  3. Build the contracts manifest (with CUAD ground-truth clauses)
  4. Start CUADBatchIngestionWorkflow in Temporal

Usage:
  # Dry run — see what will be ingested
  python run_ingestion.py --dry-run

  # Test with 5 contracts first
  python run_ingestion.py --limit 5 --concurrency 2

  # Full ingestion
  python run_ingestion.py --concurrency 5
"""
import argparse
import ast
import asyncio
import csv
import json
from collections import Counter, defaultdict
from pathlib import Path

from temporalio.client import Client

from config import (
    CUAD_CSV,
    CUAD_PDF_PARTS,
    TEMPORAL_HOST,
    TEMPORAL_NAMESPACE,
    TEMPORAL_TASK_QUEUE,
    RISK_CLAUSE_COLS,
    get_answer_col,
)


# ── PDF finder ────────────────────────────────────────────────────────────────

def find_pdf(filename: str) -> Path | None:
    """
    Search Part_I / Part_II / Part_III for the PDF.
    Case-insensitive to handle .PDF vs .pdf variants.
    """
    lower = filename.lower()
    for part_dir in CUAD_PDF_PARTS:
        if not part_dir.exists():
            continue
        ## rglob searches every subdirectory and finds the file regardless of where exactly it sits, no path reconstruction needed.
        for pdf in part_dir.rglob("*"):
            if pdf.name.lower() == lower:
                return pdf
    return None


# ── CSV parsing ───────────────────────────────────────────────────────────────

def parse_clause_texts(raw: str) -> list[str]:
    """
    CSV cells store clause text as a Python list literal, e.g.:
      "['text one', 'text two']"
    Parse it safely with ast.literal_eval.
    """
    raw = raw.strip()
    if not raw or raw == "[]":
        return []
    try:
        parsed = ast.literal_eval(raw)
        return [str(t) for t in parsed if str(t).strip()]
    except Exception:
        # Fallback: treat the whole thing as one string
        return [raw] if raw else []

## Returns two things: the contracts list (what succeeded) and the missing list (what failed to find a PDF).
def load_contracts_from_csv(limit: int | None = None) -> tuple[list, list]:
    """
    Read master_clauses.csv, match to PDFs on disk,
    return (contracts, missing_files).

    Each contract dict:
      {
        "local_pdf_path": str,
        "contract_type":  str,
        "source":         "cuad",
        "cuad_clauses":   [{"clause_type", "text", "answer", "start_char"}]
      }
    """
    contracts = []
    missing   = []

    with open(CUAD_CSV, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)

        for row in reader:
            filename = row["Filename"]
            pdf_path = find_pdf(filename)

            if not pdf_path:
                missing.append(filename)
                continue

            # Extract contract type from filename:
            # e.g. "...Affiliate Agreement.pdf" → "Affiliate Agreement"
            # CUAD has both lowercase .pdf and uppercase .PDF filenames. Handling both prevents the .PDF from appearing in the contract type string.
            stem          = filename.replace(".pdf", "").replace(".PDF", "")
            contract_type = stem.split("_")[-1].strip()

            # Build ground-truth clauses from the CSV columns
            cuad_clauses = []
            for col in RISK_CLAUSE_COLS:
                ans_col = get_answer_col(col)
                answer  = row.get(ans_col, "").strip()
                raw_txt = row.get(col, "").strip()

                ## Filtering on answer == "Yes" ensures you only store clauses that are actually present and confirmed by a human reviewer.
                if answer == "Yes":
                    texts = parse_clause_texts(raw_txt)
                    for text in texts:
                        if text:
                            cuad_clauses.append({
                                "clause_type": col,
                                "text":        text,
                                "answer":      "Yes",
                                "start_char":  None,
                            })

            contracts.append({
                "local_pdf_path": str(pdf_path),
                "contract_type":  contract_type,
                "source":         "cuad",
                "cuad_clauses":   cuad_clauses,
            })

            if limit and len(contracts) >= limit:
                break

    return contracts, missing


# ── Summary printer ───────────────────────────────────────────────────────────

def print_summary(contracts: list, missing: list) -> None:
    print(f"\n{'='*60}")
    print(f"CUAD INGESTION MANIFEST SUMMARY")
    print(f"{'='*60}")
    print(f"Contracts found : {len(contracts)}")
    print(f"PDFs missing    : {len(missing)}")

    if missing:
        print(f"\nMissing files (first 5):")
        for f in missing[:5]:
            print(f"  ✗ {f[:80]}")

    # By contract type
    types = Counter(c["contract_type"] for c in contracts)
    print(f"\nTop 10 contract types:")
    for ctype, count in types.most_common(10):
        print(f"  {ctype:40} {count:3}")

    # Risk clause distribution
    clause_counts: dict[str, int] = defaultdict(int)
    for c in contracts:
        for cl in c["cuad_clauses"]:
            clause_counts[cl["clause_type"]] += 1

    print(f"\nRisk clause distribution (ground truth):")
    for clause, count in sorted(clause_counts.items(), key=lambda x: -x[1]):
        contracts_with_clause = sum(
            1 for c in contracts
            if any(cl["clause_type"] == clause for cl in c["cuad_clauses"])
            )
        pct = contracts_with_clause / len(contracts) * 100
        print(f"  {clause:45} {contracts_with_clause:3} contracts ({pct:.0f}%) | {count} excerpts")

    # Top 3 highest-risk contracts
    top = sorted(contracts,key=lambda x: -len({cl["clause_type"] for cl in x["cuad_clauses"]}))[:3]
    print(f"\nTop 3 highest-risk contracts:")
    for c in top:
        name = Path(c["local_pdf_path"]).stem[:60]
        print(f"  {name}")
        unique_types = list({cl["clause_type"] for cl in c["cuad_clauses"]})
        print(f"    Unique risk types: {len(unique_types)} | Excerpts: {len(c['cuad_clauses'])}")
        print(f"    Types: {', '.join(unique_types)}")
    print(f"{'='*60}\n")


# ── Main ──────────────────────────────────────────────────────────────────────

async def main() -> None:
    parser = argparse.ArgumentParser(
        description="Load CUAD contracts and start Temporal ingestion"
    )
    parser.add_argument(
        "--limit", type=int, default=None,
        help="Max contracts to ingest (default: all 499)"
    )
    parser.add_argument(
        "--concurrency", type=int, default=5,
        help="Parallel child workflows (default: 5)"
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Print summary without starting Temporal workflow"
    )
    parser.add_argument(
        "--save-manifest", action="store_true",
        help="Save contracts manifest to eval/contracts_manifest.json"
    )
    args = parser.parse_args()

    print(f"Reading CUAD CSV: {CUAD_CSV}")
    contracts, missing = load_contracts_from_csv(limit=args.limit)
    print_summary(contracts, missing)

    if args.save_manifest:
        out = Path("eval/contracts_manifest.json")
        out.parent.mkdir(exist_ok=True)
        out.write_text(json.dumps(contracts, indent=2))
        print(f"Saved manifest → {out}")

    if args.dry_run:
        print("Dry run — not starting Temporal workflow.")
        return

    if not contracts:
        print("No contracts to ingest. Check CUAD_ROOT in your .env")
        return

    # Start Temporal workflow
    print(f"Connecting to Temporal at {TEMPORAL_HOST}...")
    client = await Client.connect(TEMPORAL_HOST, namespace=TEMPORAL_NAMESPACE)

    workflow_id = f"cuad-ingestion-{len(contracts)}-contracts"

    handle = await client.start_workflow(
        "CUADBatchIngestionWorkflow",
        args=[{
            "contracts":   contracts,
            "concurrency": args.concurrency,
        }],
        id=workflow_id,
        task_queue=TEMPORAL_TASK_QUEUE,
    )

    print(f"\nWorkflow started!")
    print(f"  ID:       {workflow_id}")
    print(f"  UI:       http://localhost:8080")
    print(f"  Contracts: {len(contracts)}")
    print(f"  Concurrency: {args.concurrency}")
    print(f"\nPolling progress every 15s (Ctrl+C to detach)...")

    try:
        while True:
            await asyncio.sleep(15)
            try:
                progress = await handle.query(
                    "get_progress", result_type=dict
                )
                pct   = progress.get("pct", 0)
                done  = progress.get("done", 0)
                total = progress.get("total", 0)
                failed = progress.get("failed", 0)
                status = progress.get("status", "")
                print(
                    f"  [{status}] {done}/{total} ({pct}%) "
                    f"| failed: {failed}"
                )
                if status == "completed":
                    break
            except Exception as e:
                print(f"  Query error: {e}")
    except KeyboardInterrupt:
        print("\nDetached from workflow — it continues running in Temporal.")
        print(f"Check status at: http://localhost:8080")
        return

    result = await handle.result()
    print(f"\n{'='*60}")
    print(f"INGESTION COMPLETE")
    print(f"  Succeeded: {result['succeeded']}")
    print(f"  Failed:    {result['failed']}")
    print(f"  Total:     {result['total']}")
    print(f"{'='*60}")


if __name__ == "__main__":
    asyncio.run(main())