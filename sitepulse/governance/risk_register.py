"""Operational Risk Register & Mitigation Controls.

Documents potential technical, telemetry, and analytical failure modes,
their probabilities, business impacts, and automated engineering mitigations.
"""
from dataclasses import dataclass
from typing import List, Dict, Any


@dataclass
class RiskItem:
    risk_id: str
    title: str
    category: str  # Telemetry, Data Pipeline, AI Governance, Financial Analytics, Identity Compliance
    probability: str  # LOW, MEDIUM, HIGH
    impact: str       # LOW, MEDIUM, HIGH, CRITICAL
    risk_score: int   # 1 to 25
    business_consequence: str
    engineering_mitigation: str
    control_owner: str
    residual_risk: str


class RiskRegister:
    """Enterprise Risk Register for Digital Analytics and Content Ops."""

    def __init__(self):
        self._risks: List[RiskItem] = []
        self._bootstrap_risks()

    def _bootstrap_risks(self) -> None:
        self._risks = [
            RiskItem(
                risk_id="RSK-001",
                title="Client-Side Telemetry Schema Drift",
                category="Telemetry",
                probability="HIGH",
                impact="HIGH",
                risk_score=20,
                business_consequence="Tag manager updates rename conversion events; downstream attribution shows $0 revenue.",
                engineering_mitigation="PySpark strict schema contract validation; automatic isolation into Dead Letter Queue (DLQ); schema stability alerting.",
                control_owner="Senior Data Engineer (Content Ops)",
                residual_risk="LOW"
            ),
            RiskItem(
                risk_id="RSK-002",
                title="Silent Mobile Telemetry Drop",
                category="Telemetry",
                probability="MEDIUM",
                impact="CRITICAL",
                risk_score=18,
                business_consequence="Ad platforms down-bid mobile inventory due to untracked conversions; direct revenue loss.",
                engineering_mitigation="Business Metric Anomaly Engine measuring device divergence and checkout touch completion rates.",
                control_owner="Lead Site Quality Engineer",
                residual_risk="LOW"
            ),
            RiskItem(
                risk_id="RSK-003",
                title="Double-Firing Tag Multi-Counting",
                category="Data Pipeline",
                probability="MEDIUM",
                impact="HIGH",
                risk_score=15,
                business_consequence="Inflated conversion rates lead to over-spending on ineffective advertising channels.",
                engineering_mitigation="PySpark windowed deduplication over (visitor_id, event_name, event_time) within 30-min window.",
                control_owner="Data Engineering Lead",
                residual_risk="VERY_LOW"
            ),
            RiskItem(
                risk_id="RSK-004",
                title="Missing Identity / Visitor Null Explosion",
                category="Identity Compliance",
                probability="MEDIUM",
                impact="HIGH",
                risk_score=16,
                business_consequence="CMP consent banner race conditions strip visitor IDs; multi-touch attribution fails.",
                engineering_mitigation="Quarantine filtering for null visitor_id; Completeness dimension degradation alerts.",
                control_owner="Analytics Engineer",
                residual_risk="LOW"
            ),
            RiskItem(
                risk_id="RSK-005",
                title="Outlier / Bogus Revenue Injections",
                category="Financial Analytics",
                probability="LOW",
                impact="CRITICAL",
                risk_score=15,
                business_consequence="Corrupted financial reporting to executive leadership; erroneous revenue recognition.",
                engineering_mitigation="PySpark range assertion quarantine (0.01 <= revenue <= 50,000.0); automatic DLQ isolation.",
                control_owner="Senior Data Engineer",
                residual_risk="VERY_LOW"
            ),
            RiskItem(
                risk_id="RSK-006",
                title="Unsafe or Destructive AI-Generated SQL",
                category="AI Governance",
                probability="MEDIUM",
                impact="HIGH",
                risk_score=16,
                business_consequence="Hallucinated LLM queries could drop serving tables or leak confidential data.",
                engineering_mitigation="sqlglot Abstract Syntax Tree (AST) validation enforcing read-only SELECT, table allowlists, and row limits.",
                control_owner="AI/ML Platform Architect",
                residual_risk="VERY_LOW"
            ),
            RiskItem(
                risk_id="RSK-007",
                title="Hallucinated Root-Cause Analysis",
                category="AI Governance",
                probability="MEDIUM",
                impact="HIGH",
                risk_score=14,
                business_consequence="Operations teams spend engineering hours chasing false root causes.",
                engineering_mitigation="Evidence-grounded cross-layer correlation combining DLQ logs, health scores, and funnel deltas.",
                control_owner="Data Quality Architect",
                residual_risk="LOW"
            )
        ]

    def list_risks(self) -> List[RiskItem]:
        return self._risks

    def get_risk_summary(self) -> Dict[str, Any]:
        return {
            "total_risks_monitored": len(self._risks),
            "high_critical_risks": sum(1 for r in self._risks if r.impact in ("HIGH", "CRITICAL")),
            "mitigation_coverage_pct": 100.0,
            "risks_by_category": {
                cat: sum(1 for r in self._risks if r.category == cat)
                for cat in set(r.category for r in self._risks)
            }
        }
