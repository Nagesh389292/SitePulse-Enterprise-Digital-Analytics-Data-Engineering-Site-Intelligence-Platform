# SitePulse — Enterprise Digital Analytics, Data Quality & Business Intelligence Platform

[![Python](https://img.shields.io/badge/Python-3.11%20%7C%203.12%20%7C%203.13-blue.svg)](https://www.python.org/)
[![PySpark](https://img.shields.io/badge/PySpark-3.5+-orange.svg)](https://spark.apache.org/)
[![Apache Iceberg](https://img.shields.io/badge/Lakehouse-Apache%20Iceberg-blue.svg)](https://iceberg.apache.org/)
[![Airflow](https://img.shields.io/badge/Orchestration-Apache%20Airflow-teal.svg)](https://airflow.apache.org/)
[![DuckDB / SQL Server](https://img.shields.io/badge/Serving-DuckDB%20%7C%20SQL%20Server-red.svg)](https://duckdb.org/)
[![Power BI](https://img.shields.io/badge/BI-Power%20BI%20DAX-yellow.svg)](https://powerbi.microsoft.com/)
[![Tests](https://img.shields.io/badge/Automated%20Tests-35%2F35%20Passing%20(100%25)-brightgreen.svg)](tests/)

SitePulse is an enterprise-grade digital analytics reliability and business intelligence platform. It bridges the critical divide between engineering telemetry and business impact by detecting silent data breakages, schema drift, and mobile tag drops, quantifying their financial revenue exposure in USD ($), and triggering autonomous AI Root Cause Analysis (RCA) within an ITIL-aligned incident lifecycle.

---

## 1. The Enterprise Problem & Complete Value Chain

Traditional Data Quality tools only evaluate structural syntax (e.g. valid schemas, low null counts). When a bad frontend release silently drops mobile purchase tags, the ETL pipeline reports `SUCCESS` and standard DQ returns `100/100`, yet mobile conversion rate collapses to `0.0%`, distorting attribution and wasting tens of thousands of dollars in ad spend.

SitePulse implements the complete 14-stage enterprise chain:
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

## 2. Platform Architecture & Components

| Layer | Technologies | Responsibilities | Key Files |
| :--- | :--- | :--- | :--- |
| **Ingestion** | Python, GA4 Clickstream | Realistic e-commerce flows, 6 deterministic failure injections | `sitepulse/generator/` |
| **ETL & Lakehouse** | PySpark, Apache Iceberg | 30-min inactivity sessionization, window deduplication, ACID snapshot commits | `sitepulse/pyspark_engine/`, `sitepulse/lakehouse/` |
| **Orchestration** | Apache Airflow | Micro-batch DAG with sensor, branch evaluation, and alerting | `dags/sitepulse_orchestration_dag.py` |
| **Serving Marts** | DuckDB, SQL Server | Star-schema dimensional model with Clustered Columnstore Indexes | `sitepulse/serving/`, `sqlserver/` |
| **Data Quality** | Mathematical Engines | Dual-layer reliability: 0–100 Structural DQ + Behavioral Anomaly Engine | `sitepulse/quality_engine/` |
| **Analytics & BI** | Scipy, Power BI DAX | Two-sample proportion Z-test, Tukey's IQR outlier test, DAX measures | `sitepulse/analytics/`, `powerbi/` |
| **AI & Security** | AST sqlglot, Vector RAG | AST SQL injection guard, governed NL-to-SQL compiler, autonomous RCA | `sitepulse/ai_agent/` |
| **Operations** | ITIL State Machine | DETECTED $\to$ TRIAGED $\to$ INVESTIGATING $\to$ ROOT_CAUSE $\to$ CLOSED | `sitepulse/incident_mgmt/` |
| **Governance** | Data Catalog, Risk Register | Data Dictionary (SLAs, PII sensitivity) and 7 operational risk controls | `sitepulse/governance/` |
| **Deployment** | Docker, AWS ECS Fargate | Multi-stage Docker container, docker-compose, and ECS task definitions | `Dockerfile`, `docker-compose.yml`, `ecs/` |

---

## 3. Quick Start & Execution

### Running the End-to-End Demonstration
Execute the full 14-stage enterprise pipeline in terminal:
```bash
python run_e2e_demonstration.py
```

### Running the Full Test Suite
Execute the 35 automated tests verifying PySpark, Iceberg, Serving, Quality, Anomaly, AST Guard, and AI RCA:
```bash
python -m pytest -v
```

### Starting the Web API & Modern UI Dashboard
```bash
python -m uvicorn sitepulse.api.app:app --host 0.0.0.0 --port 8000
```
Open your browser at `http://localhost:8000/` to inspect:
* Real-time 0–100 Multi-Layer Reliability Score
* Interactive Failure Scenario injection buttons
* Governed AST-validated Natural Language to SQL console
* ITIL Incident Lifecycle timeline & Risk Register
* Data Governance Catalog

---

## 4. Docker & AWS ECS Deployment

### Local Docker Compose
```bash
docker-compose up --build -d
```

### AWS ECS Fargate
Refer to `ecs/deploy_guide.md` and `ecs/task-definition.json` for production deployment instructions on AWS ECS Fargate with persistent EFS storage.

---

## 5. Documentation & Interview Defense
* [Technical Architecture Specification](file:///C:/Users/NAGESH%20REDDY/.gemini/antigravity-ide/brain/fad69b32-1ea2-4536-b05f-192160f66f6f/SitePulse_Technical_Architecture_Spec.md)
* [Enterprise Production Documentation](file:///C:/Users/NAGESH%20REDDY/.gemini/antigravity-ide/brain/fad69b32-1ea2-4536-b05f-192160f66f6f/SitePulse_Enterprise_Production_Documentation.md)
* [Executive Presentation Deck](file:///C:/Users/NAGESH%20REDDY/.gemini/antigravity-ide/scratch/sitepulse/docs/SitePulse_Executive_Presentation_Deck.md)
* [Technical Interview Defense Manual](file:///C:/Users/NAGESH%20REDDY/.gemini/antigravity-ide/scratch/sitepulse/docs/SitePulse_Interview_Defense_Manual.md)
