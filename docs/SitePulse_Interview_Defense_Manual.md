# SitePulse — Technical Interview Defense Manual

This guide prepares you to defend every engineering and architectural decision in SitePulse across Senior Data Engineer, Analytics Engineer, and BI Architect interview rounds.

---

## Part 1: High-Level Framing (The Opening Question)

### Q: "Tell me about a project where you solved a real business problem."
**Defense Strategy**:
Walk them through the complete business-to-engineering chain. Do not start with technologies; start with the business risk:

> *"At enterprise scale, digital analytics pipelines frequently suffer from silent telemetry failures—such as a frontend deployment accidentally dropping mobile checkout purchase tags. Traditional data pipelines continue to succeed because schemas remain valid, nulls don't explode, and records still flow. However, marketing campaigns bid blindly and executive attribution is completely corrupted, resulting in tens of thousands of dollars in wasted ad spend.*
>
> *To solve this, I designed and built **SitePulse**, an enterprise digital analytics, data quality, and business intelligence platform grounded in Google Merchandise Store GA4 event standards. SitePulse implements a 14-step automated chain:
>
> 1. We ingest high-volume clickstream events and run a PySpark 30-minute inactivity sessionizer with window deduplication and dead-letter queue (DLQ) quarantine.
> 2. We commit Bronze and Silver tables into an Apache Iceberg Lakehouse with full ACID snapshot history and time-travel rollback capability.
> 3. We load a columnar star-schema serving layer in DuckDB / SQL Server and Power BI DAX semantic model.
> 4. Most importantly, we implemented a dual-layer quality engine: structural DQ (Freshness, Completeness, Schema, Volume, Referential) combined with a Behavioral Anomaly Engine using Two-Sample Proportion Z-tests.
> 5. When mobile conversions diverge from desktop, SitePulse automatically generates an ITIL incident record, models the financial revenue exposure in dollars, triggers an AST-guarded AI Root Cause Analysis (RCA) agent to diagnose the issue, and alerts Content Ops with a specific remediation plan within 15 minutes."*

---

## Part 2: Data Engineering Deep Dive

### Q: "How does your PySpark sessionization logic handle out-of-order events and late-arriving data?"
**Defense**:
* We partition by `visitor_id` and order by timestamp `ts`.
* Inactivity timeout calculation: We compute the difference in seconds between the current event and the preceding event:
  $$\Delta t = \text{unix\_timestamp}(ts) - \text{lag}(\text{unix\_timestamp}(ts), 1)$$
* If $\Delta t > 1800$ (30 minutes) or is null (first session event), we mark `is_new_session = 1`, else `0`.
* We take the cumulative sum of `is_new_session` over the ordered visitor window to generate a deterministic integer session index.
* For deduplication, we use a window partitioned by `event_id` and ordered by event timestamp ascending, filtering for `row_number() == 1`.
* Any payload failing schema constraints or containing null visitor IDs is isolated into the quarantine DLQ without crashing the pipeline.

### Q: "Why Apache Iceberg? What specific problem does it solve in this pipeline?"
**Defense**:
* In digital analytics, data correction and audit compliance are constant requirements (e.g. reprocessing late-arriving ad conversions or GDPR purge requests).
* Apache Iceberg provides ACID transactions at the table level with snapshot isolation.
* In SitePulse, every PySpark micro-batch commits an Iceberg snapshot containing manifest lists and summaries (`added-records`, `total-records`).
* If a schema corruption or bad deployment occurs, SitePulse can execute an automated rollback to a known-clean snapshot (`rollback_to_snapshot`) in milliseconds without rewriting raw files.

### Q: "How is the Airflow DAG structured?"
**Defense**:
* Our DAG `sitepulse_analytics_orchestration` runs on an hourly micro-batch schedule (`0 * * * *`).
* It orchestrates: Ingestion $\to$ PySpark Sessionization $\to$ Iceberg ACID Commit $\to$ dbt Dimensional Mart Materialization $\to$ Quality Gate Branching.
* The branch operator evaluates the 0–100 quality score:
  * If score $\ge 70$, it proceeds to behavioral anomaly detection.
  * If score $< 70$, it triggers an immediate P1 ITIL alert and dispatches to PagerDuty/Slack.

---

## Part 3: Analytics, Quality & Modeling Deep Dive

### Q: "Why wasn't a standard Great Expectations or Data Quality check enough?"
**Defense**:
* Standard DQ checks are purely **structural**: they check schema types, null percentages, and row counts.
* In our `FAIL_MOBILE_DROP` failure scenario, a frontend release dropped the purchase event on mobile viewports.
* The events that did arrive were perfectly valid JSON, had zero nulls, and schema validation gave a **100/100 score**.
* However, from a business perspective, the mobile conversion rate collapsed to **0.0%** while desktop was healthy at **2.5%**.
* SitePulse's `BusinessMetricAnomalyEngine` executes a Two-Sample Proportion Z-Test comparing segments. It detects that the conversion rate divergence has a $p$-value $< 0.001$, triggering an automated incident despite the 100/100 structural score.

### Q: "Explain how you calculate Revenue Exposure."
**Defense**:
* We calculate the conversion gap:
  $$\text{Conversion Gap} = \max(0, \text{Desktop CR} - \text{Mobile CR})$$
* Expected lost orders:
  $$\text{Lost Orders} = \text{Mobile Sessions} \times \text{Conversion Gap}$$
* Revenue exposure:
  $$\text{Revenue Exposure (\$) } = \text{Lost Orders} \times \text{Observed Average Order Value (AOV)}$$
* In our automated failure scenario, this models an instantaneous exposure of over $40,000, allowing Content Ops to prioritize the incident based on financial materiality.

---

## Part 4: AI & Security Deep Dive

### Q: "How do you prevent SQL injection and hallucinations in your NL-to-SQL layer?"
**Defense**:
* We never pass raw LLM-generated SQL directly to the database.
* All queries pass through `SQLGuard`, which parses the SQL into an Abstract Syntax Tree (AST) using `sqlglot`.
* Security invariants enforced:
  1. **Strictly SELECT**: Rejects `DROP`, `DELETE`, `UPDATE`, `INSERT`, `ALTER`, `TRUNCATE`.
  2. **Table Allowlist**: Only allows queries against verified dimensional tables (`dim_page`, `dim_device`, `dim_campaign`, `fact_sessions`, `fact_conversions`).
  3. **Multi-Statement Blocking**: Rejects semicolons or batched queries.
  4. **Filesystem Isolation**: Blocks DuckDB/SQL Server filesystem reading functions (e.g. `read_parquet`, `read_csv`, `xp_cmdshell`).
  5. **Query Clamping**: Enforces a mandatory `LIMIT 500` to prevent runaway memory consumption.

### Q: "How does the autonomous RCA Agent identify the root cause?"
**Defense**:
* The RCA Agent cross-correlates two independent telemetry streams:
  1. The serving layer's statistical anomalies (e.g. mobile conversion drop).
  2. The lakehouse DLQ quarantine logs and error codes.
* When a schema drift or missing tag occurs, the RCA Agent correlates the surge in quarantine error codes with the timing of the conversion drop, formulating an exact root-cause diagnosis and actionable remediation within seconds.
