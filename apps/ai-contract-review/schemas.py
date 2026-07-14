from pydantic import BaseModel,Field
from typing import Literal

class PDFSummaryChema(BaseModel):
    summary: str = Field(
        ...,
        description="2-3 sentence plain-English summary of what this contract "
                    "covers and the main obligations of each party",
    )
    key_risks: str = Field(
        ...,
        description="Bullet list of the top 3-5 risks, one per line, "
                    "starting with a dash",
    )

class CrossContractRisk(BaseModel):
    risk: str = Field(..., description="Description of the compounding/cross-contract risk")
    affected_contracts: list[str] = Field(..., description="s3_paths or identifiers of affected contracts")


class SynthesisReportSchema(BaseModel):
    overall_risk_level: Literal["High", "Medium", "Low"]
    risk_justification: str = Field(..., description="One sentence justifying the overall_risk_level rating")
    top_cross_contract_risks: list[CrossContractRisk] = Field(..., max_length=8)
    recommended_actions: list[str] = Field(..., description="Concrete steps the legal team should take")

SCHEMA_REGISTRY:dict[str,type[BaseModel]]={
    "PDFSummarySchema":PDFSummaryChema,
    "SynthesisReportSchema":SynthesisReportSchema
}