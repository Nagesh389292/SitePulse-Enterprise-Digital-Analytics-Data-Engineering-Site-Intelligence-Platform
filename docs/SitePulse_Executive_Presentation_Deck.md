# SitePulse — Executive & Technical Presentation Deck

**Platform Title**: SitePulse: Enterprise Digital Analytics, Data Quality & Business Intelligence Platform  
**Target Roles**: Senior Data Engineer (Content Ops & Site Quality) / Analytics Engineer / BI Architect  
**Architecture**: PySpark Lakehouse (Apache Iceberg) + Airflow + SQL Server / DuckDB Columnstore + Power BI + AST-Guarded AI/RCA  

---

## Slide 1: Executive Title & Value Proposition
* **Header**: Bridging the Gap Between Engineering Telemetry and Business Impact
* **Subtitle**: How SitePulse Prevents Undetected Revenue Leakage in Enterprise Digital Analytics
* **Core Metrics**:
  * 0–100 Multi-Layer Reliability Score (Structural DQ + Behavioral Anomaly)
  * MTTD (Mean Time to Detect) reduced from 48–72 hours to < 15 minutes
  * Automated financial exposure quantification ($25,000–$100,000 protected per release)
  * End-to-end ITIL Incident Lifecycle with Autonomous AI Root Cause Analysis

---

## Slide 2: The Enterprise Business Problem
* **The Reality**: Marketing and e-commerce teams spend millions on paid acquisition. A minor frontend release (e.g., bad tag firing, unescaped payload, or dropped purchase event) silently destroys mobile conversion tracking.
* **The Blind Spot**: Traditional Data Quality tools check:
  * *Are schemas valid?* (Yes, fields are strings)
  * *Are records null?* (No, null counts are 0)
  * *Did the ETL fail?* (No, DAG status is SUCCESS)
* **The Failure**: Meanwhile, real mobile conversions drop to **0.0%**. Paid search algorithms bid blindly, attribution reports mislead CMOs, and thousands of dollars in ad spend are wasted daily.

---

## Slide 3: The Complete Enterprise Chain (End-to-End)
```text
Business Problem
      ↓
Requirements Engineering (FR-01 to FR-05, NFR-01 to NFR-03)
      ↓
Process Analysis (As-Is Reactive vs To-Be Proactive)
      ↓
Data Ingestion (Google Merchandise Store GA4 standard)
      ↓
PySpark ETL/ELT (30-min window sessionization, deduplication, DLQ quarantine)
      ↓
Apache Iceberg Lakehouse (ACID snapshot commits, lineage, time-travel rollback)
      ↓
Airflow Orchestration (Micro-batch DAG, Quality Gates, Alerting)
      ↓
SQL Server / DuckDB Columnstore (Star-Schema: dim_page, dim_device, fact_sessions)
      ↓
Data Quality & Governance (0-100 Structural Score, Data Catalog, Risk Register)
      ↓
Statistical / KPI Analysis (Two-Sample Proportion Z-Test, Variance Engine)
      ↓
Power BI Semantic Layer (Enterprise DAX Measures, BIM Tabular Model)
      ↓
AI / RAG / NL → SQL (sqlglot AST Security Guard, Read-Only Allowlist)
      ↓
Root Cause Analysis (RCA Investigator cross-correlating telemetry & DLQ)
      ↓
Incident / Risk Management (ITIL State Machine, Risk Register Controls)
      ↓
Business Recommendations (Executive Rollback, Marketing Shift, CI/CD Gate)
```

---

## Slide 4: Data Engineering Core — PySpark & Apache Iceberg
* **PySpark 30-Minute Inactivity Sessionization**:
  * Window specification: `Window.partitionBy("visitor_id").orderBy("ts")`
  * Inactivity calculation: `when((unix_timestamp(ts) - lag(unix_timestamp(ts))) > 1800, 1).otherwise(0)`
  * Cumulative session indexing: `spark_sum("is_new_session").over(...)`
* **Apache Iceberg Lakehouse Integration**:
  * ACID transaction logging across Bronze (`bronze_events`), Silver (`silver_sessions`, `silver_conversions`), and DLQ (`quarantine_dlq`).
  * Instant time-travel rollback capability (`rollback_to_snapshot`) ensuring audit compliance.

---

## Slide 5: The Dual Quality Engine — Structural vs Behavioral
| Layer | What It Checks | Metric / Formula | Scenario: Mobile Tag Drop |
| :--- | :--- | :--- | :--- |
| **Structural DQ** | Freshness, Completeness, Schema, Volume, Referential | `DQ = \sum w_i S_i` (0–100) | **100/100 (Clean)** |
| **Behavioral Engine** | Device divergence, zero conversion rates, basket size | Two-Sample Proportion Z-Test ($p < 0.05$) | **40/100 (CRITICAL ANOMALY)** |
| **Composite Platform** | Blended Enterprise Reliability | `Score = 0.40 \times DQ + 0.60 \times Behavioral` | **64.0/100 [DEGRADED]** |

---

## Slide 6: Statistical Inference & Financial Impact Modeling
* **Statistical Rigor**:
  * Pooled Two-Sample Z-Test comparing Desktop vs Mobile conversion rates:
    $$Z = \frac{\hat{p}_{desk} - \hat{p}_{mob}}{\sqrt{\hat{p}(1-\hat{p})(\frac{1}{n_{desk}} + \frac{1}{n_{mob}})}}$$
  * When $p < 0.01$, SitePulse rejects $H_0$ and flags an enterprise anomaly.
* **Financial Exposure Quantification**:
  $$\text{Expected Orders} = \text{Mobile Sessions} \times (\text{Baseline CR}_{desk} - \text{Observed CR}_{mob})$$
  $$\text{Revenue Exposure (\$) } = \text{Expected Orders} \times \text{AOV}$$
  * Directly answers the executive question: *"How much money did this bug cost us?"*

---

## Slide 7: Governed AI — AST-Guarded NL-to-SQL & Autonomous RCA
* **Security Guard via Abstract Syntax Tree (`sqlglot`)**:
  * Restricts statements strictly to `SELECT` (rejects `DROP`, `DELETE`, `UPDATE`, `ALTER`).
  * Allowlist enforcement: only queries `dim_page`, `dim_device`, `dim_campaign`, `fact_sessions`, `fact_conversions`.
  * Clamps runaway queries with mandatory `LIMIT 500`.
* **Autonomous RCA Investigator**:
  * Correlates business metric drops with PySpark DLQ quarantine error codes.
  * Formulates Root Cause diagnosis, confidence score, and remediation plan within seconds.

---

## Slide 8: Live Demonstration Walkthrough
1. Run Clean Baseline: 100/100 Structural Score, 95/100 Behavioral Score.
2. Inject `FAIL_MOBILE_DROP`: Mobile checkout tags fail.
3. System Triggers:
   * Structural DQ: 100/100
   * Behavioral Engine: Flags Mobile CR = 0.00% (p = 0.0001)
   * Composite Reliability: Drops to 64.0/100
   * ITIL Incident Auto-Generated: `INC-8921B` with $45,210 financial exposure
   * RCA Agent Identifies: Frontend checkout tag regression on mobile viewport.
   * Executive Action: Immediate rollback recommendation dispatched.

---

## Slide 9: Business ROI & Measurable Outcomes
* **90% Reduction in Detection Latency**: Mean Time To Detect (MTTD) drops from days to minutes.
* **Zero Untracked Ad Spend Waste**: Paid marketing paused automatically when conversion telemetry collapses.
* **Cross-Functional Alignment**: Single source of truth connecting Data Engineering, Analytics, Content Ops, and Finance.
