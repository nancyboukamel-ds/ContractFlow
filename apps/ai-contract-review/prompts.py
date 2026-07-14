import textwrap

_SUMMARY_PROMPT = textwrap.dedent("""\
    You are a legal analyst reviewing a contract excerpt.

    Identify the key obligations, rights, and risks for the parties involved.

    Return ONLY a JSON object with exactly these two fields — no markdown, no code block:
    {{
      "summary": "2-3 sentence plain-English summary of what this contract covers and the main obligations of each party",
      "key_risks": "bullet list of the top 3-5 risks, one per line, starting with a dash (e.g. - Risk description)"
    }}

    Contract text:
    {text}
                                  
    # Output:
    
    ```json                              
    """)

_SYNTHESIS_PROMPT = textwrap.dedent("""\
    You are a senior legal analyst preparing a consolidated risk report for a legal team.

    {n} contracts have been individually analyzed. Your task is to:
    - Identify cross-contract patterns and compounding risks
    - Assess the overall risk exposure across the entire batch
    - Recommend specific, actionable steps the legal team should take before signing

    {summaries}

    Return ONLY a JSON object matching this exact schema - no markdown, no code block:
    {{
      "overall_risk_level": "High or Medium or Low - exactly one of these three words.",
      "risk_justification": "One sentence justifying the overall_risk_level rating",
      "top_cross_contract_risks": [
        {{
          "risk": "Description of the compounding or cross-contract risk",
          "affected_contracts": ["s3://...", "s3://..."]
        }}
      ],
      "recommended_actions": [
        "Step 1: ...",
        "Step 2: ..."
      ]
    }}

    Requirements:
    - overall_risk_level must be exactly one of: High, Medium, Low
    - top_cross_contract_risks must be a JSON array of objects, max 8 items
    - recommended_actions must be a JSON array of strings
    - Do not include any text outside the JSON object

    """)

_REVISION_PROMPT = textwrap.dedent("""\
    You are a senior legal analyst. A reviewer has requested changes to the risk report below.

    Rewrite the report in full, incorporating the reviewer's feedback.
    Preserve the same JSON schema as the current report.

    --- CURRENT REPORT ---
    {report}

    --- REVIEWER FEEDBACK ---
    {feedback}

    Return ONLY a JSON object matching this exact schema - no markdown, no code block:
    {{
      "overall_risk_level": "High or Medium or Low — exactly one of these three words",
      "risk_justification": "One sentence justifying the overall_risk_level rating",
      "top_cross_contract_risks": [
        {{
          "risk": "Description of the compounding or cross-contract risk",
          "affected_contracts": ["s3://...", "s3://..."]
        }}
      ],
      "recommended_actions": [
        "Step 1: ...",
        "Step 2: ..."
      ]
    }}

    Requirements:
    - overall_risk_level must be exactly one of: High, Medium, Low
    - top_cross_contract_risks must be a JSON array of objects, exactly 3 items
    - recommended_actions must be a JSON array of strings
    - Apply the reviewer feedback to all relevant fields
    - Do not include any text outside the JSON object
    """)