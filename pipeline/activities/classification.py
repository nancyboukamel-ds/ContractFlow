"""
pipeline/activities/classification.py
LLM-based clause detection activity.
Uses the LLM to identify which CUAD clause types are present in a contract
and extracts the relevant text for each.
"""

## This file contains one activity that uses the LLM to detect which risk clause types are present in a contract and extract the relevant text for each.
## When a real user uploads their own PDF, there are no CUAD labels the LLM is the only way to detect clauses. 
## Second, even for CUAD contracts, the LLM detection gives you a second signal you can compare against the ground truth that comparison is your eval score
import json
import os
from dataclasses import dataclass

import json_repair
from openai import OpenAI
from temporalio import activity

from config import RISK_CLAUSE_COLS, OPENROUTER_MODEL


# ── Dataclasses ───────────────────────────────────────────────────────────────

@dataclass
class ClassifyInput:
    contract_id:   str
    contract_text: str    # first ~8000 chars of extracted text
    contract_type: str    # e.g. "Affiliate Agreement" — gives LLM context


@dataclass
class DetectedClause:
    clause_type: str
    present:     bool
    excerpt:     str       # relevant text from the contract
    confidence:  float     # 0.0 – 1.0


# ── Activity ──────────────────────────────────────────────────────────────────

@activity.defn
async def classify_clauses(params: ClassifyInput) -> list[DetectedClause]:
    """
    Ask the LLM to identify which risk clause types are present
    and extract the relevant text excerpt for each.

    Returns a list of DetectedClause — only clauses with present=True
    should be stored.
    """
    activity.heartbeat({
        "stage":       "classifying",
        "contract_id": params.contract_id,
        "text_chars":  len(params.contract_text),
    })

    client = OpenAI(
        api_key=os.environ["OPENROUTER_API_KEY"],
        base_url="https://openrouter.ai/api/v1",
    )

    clause_list = "\n".join(f"- {c}" for c in RISK_CLAUSE_COLS)

    prompt = f"""You are an expert legal contract analyst.

Contract type: {params.contract_type}

Analyze the following contract text and identify which of these clause types
are present. For each clause type that IS present, extract the most relevant
sentence or two (max 300 characters).

Clause types to detect:
{clause_list}

Contract text:
{params.contract_text[:8000]}

Return ONLY a JSON array. Each item must have exactly these fields:
[
  {{
    "clause_type": "<exact name from the list above>",
    "present": true or false,
    "excerpt": "<relevant text from the contract, empty string if not present>",
    "confidence": <float between 0.0 and 1.0>
  }}
]

Rules:
- Include ALL clause types from the list, even ones not present (present=false)
- Only extract text that is directly in the contract
- If a clause is not present, set present=false, excerpt="" , confidence=0.0
- Do not include any text outside the JSON array
"""

    response = client.chat.completions.create(
        model=os.environ.get("OPENROUTER_MODEL", OPENROUTER_MODEL),
        messages=[{"role": "user", "content": prompt}],
        max_tokens=4000,
    )

    content = response.choices[0].message.content
    ## LLMs sometimes wrap JSON in markdown code fences ```json or add trailing commas. json_repair handles all of these gracefully instead of crashing.
    raw     = json_repair.loads(content)

    if not isinstance(raw, list):
        activity.logger.warning(f"LLM returned non-list: {content[:200]}")
        return []

    results = []
    for item in raw:
        try:
            results.append(DetectedClause(
                clause_type = str(item.get("clause_type", "")),
                present     = bool(item.get("present", False)),
                ## The prompt asks for max 300 characters but LLMs don't always respect length limits. 
                excerpt     = str(item.get("excerpt", ""))[:500],
                confidence  = float(item.get("confidence", 0.5)),
            ))
        except Exception as e:
            activity.logger.warning(f"Skipping malformed clause item: {e}")
            continue

    present = [r for r in results if r.present]
    activity.logger.info(
        f"Classification done: {len(present)}/{len(RISK_CLAUSE_COLS)} "
        f"clauses found in {params.contract_id}"
    )

    return results