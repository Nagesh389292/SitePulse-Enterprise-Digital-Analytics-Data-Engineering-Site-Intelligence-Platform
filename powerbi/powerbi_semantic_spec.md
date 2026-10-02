# Power BI Semantic Model & Executive Dashboard Specification

## 1. Overview
The SitePulse Power BI solution connects directly to the analytical serving layer (DuckDB / SQL Server Columnstore) to provide executive stakeholders and site operations teams with live visibility into site reliability, device divergence, and revenue leakage.

---

## 2. Dimensional Model (Star Schema)
```text
                  ┌──────────────────────┐
                  │      dim_page        │
                  │──────────────────────│
                  │ PK: page_sk          │
                  │     url              │
                  │     path             │
                  │     title            │
                  │     content_category │
                  └──────────┬───────────┘
                             │ 1
                             │
                             │ * (landing_page_key)
┌──────────────────────┐     │     ┌──────────────────────┐
│     dim_device       │     │     │     dim_campaign     │
│──────────────────────│     │     │──────────────────────│
│ PK: device_sk        │     │     │ PK: campaign_sk      │
│     device_type      │     │     │     campaign_name    │
│     os               │     │     │     source           │
│     browser          │     │     │     medium           │
└──────────┬───────────┘     │     └──────────┬───────────┘
           │ 1               │                │ 1
           │                 │                │
           │ * (device_key)  │                │ * (campaign_key)
           └────────────┐    │    ┌───────────┘
                        ▼    ▼    ▼
                 ┌─────────────────────────────────┐
                 │          fact_sessions          │
                 │─────────────────────────────────│
                 │ PK: session_id                  │
                 │ FK: visitor_id                  │
                 │ FK: device_key                  │
                 │ FK: landing_page_key            │
                 │ FK: campaign_key                │
                 │     session_start               │
                 │     session_end                 │
                 │     duration_seconds            │
                 │     page_view_count             │
                 │     has_cart                    │
                 │     has_checkout                │
                 │     has_purchase                │
                 │     total_revenue               │
                 └────────────────┬────────────────┘
                                  │ 1
                                  │
                                  │ * (session_id)
                                  ▼
                 ┌─────────────────────────────────┐
                 │         fact_conversions        │
                 │─────────────────────────────────│
                 │ PK: conversion_id               │
                 │ FK: session_id                  │
                 │ FK: visitor_id                  │
                 │ FK: device_key                  │
                 │ FK: campaign_key                │
                 │ FK: page_key                    │
                 │     conversion_time             │
                 │     order_id                    │
                 │     revenue                     │
                 └─────────────────────────────────┘
```

---

## 3. Core Enterprise DAX Measures
Defined in `powerbi/measures.dax`:
* **`[Total Sessions]`**: `DISTINCTCOUNT(fact_sessions[session_id])`
* **`[Total Conversions]`**: `DISTINCTCOUNT(fact_conversions[order_id])`
* **`[Total Revenue]`**: `SUM(fact_conversions[revenue])`
* **`[Conversion Rate]`**: `DIVIDE([Total Conversions], [Total Sessions], 0)`
* **`[Desktop Conversion Rate]`**: `CALCULATE([Conversion Rate], dim_device[device_category] = "desktop")`
* **`[Mobile Conversion Rate]`**: `CALCULATE([Conversion Rate], dim_device[device_category] = "mobile")`
* **`[Mobile Conversion Gap]`**: `MAX(0, [Desktop Conversion Rate] - [Mobile Conversion Rate])`
* **`[Mobile Revenue Exposure ($)]`**:
  ```dax
  VAR MobileSessions = CALCULATE([Total Sessions], dim_device[device_category] = "mobile")
  VAR ExpectedConversions = MobileSessions * [Mobile Conversion Gap]
  VAR AOV = DIVIDE([Total Revenue], [Total Conversions], 65.0)
  RETURN ExpectedConversions * AOV
  ```
* **`[Composite Platform Reliability]`**: `([Site Quality Health Score] * 0.40) + ((1 - [Mobile Conversion Gap]) * 100 * 0.60)`

---

## 4. Executive Dashboard Layout (Visual Spec)
1. **Top KPI Strip**:
   * Card 1: Composite Platform Reliability (Gauge: 0–100, Target: $\ge 90$)
   * Card 2: Total Sessions & MoM Variance
   * Card 3: Mobile Conversion Rate vs Desktop Conversion Rate
   * Card 4: Financial Exposure in USD ($)
2. **Main Analytics Section**:
   * Line Chart: Hourly Conversion Rate by Device Category (Desktop vs Mobile vs Tablet)
   * Clustered Bar Chart: Conversion Funnel (Page Views $\to$ Add to Cart $\to$ Checkout $\to$ Purchase)
3. **Operations & Governance Section**:
   * Matrix Table: Landing Page Category vs Site Quality Health Score
   * Active Incidents Table: Incident ID, Severity, Status, Root Cause Headline, Assigned Team
