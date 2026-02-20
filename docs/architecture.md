# Solution Architecture — SAC Analytics Implementation

## Overview

This document describes the end-to-end architecture for the SAP Analytics Cloud analytics implementation, covering data sourcing, modeling, KPI computation, and dashboard delivery.

---

## Architecture Diagram

```
┌─────────────────────────────────────────────────────────────────────┐
│                        SOURCE SYSTEMS                               │
│   ERP (SAP S/4HANA)    CRM (Salesforce)    HRM (SuccessFactors)    │
└──────────────────────────────┬──────────────────────────────────────┘
                               │  Extract (CSV / API)
                               ▼
┌─────────────────────────────────────────────────────────────────────┐
│                     DATA STAGING LAYER                              │
│              data/raw/  (unprocessed source CSVs)                  │
│   dim_date.csv  dim_product.csv  dim_customer.csv                  │
│   dim_employee.csv  dim_region.csv  fact_sales.csv                 │
└──────────────────────────────┬──────────────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────────────┐
│                       ETL PIPELINE                                  │
│                                                                     │
│   etl/generate_sample_data.py  →  Synthetic data generation        │
│   etl/transform.py             →  Clean, enrich, FK validation     │
│   etl/validate.py              →  Data quality gate (35+ checks)   │
└──────────────────────────────┬──────────────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────────────┐
│                    STAR SCHEMA (Processed)                          │
│              data/processed/  (clean, validated CSVs)              │
│                                                                     │
│   ┌──────────┐   ┌───────────┐   ┌────────────┐                   │
│   │ dim_date │   │dim_product│   │dim_customer│                   │
│   └─────┬────┘   └─────┬─────┘   └─────┬──────┘                   │
│         │              │               │                            │
│         └──────┬────────┘───────────────┘                          │
│                ▼                                                    │
│         ┌─────────────┐   ┌────────────┐   ┌──────────────┐       │
│         │ fact_sales  ├───┤ dim_region ├───┤ dim_employee │       │
│         └─────────────┘   └────────────┘   └──────────────┘       │
└──────────────────────────────┬──────────────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────────────┐
│                    KPI CALCULATION ENGINE                           │
│                  kpis/kpi_calculator.py                            │
│                                                                     │
│   KPI-001  Total Revenue          KPI-006  Total Orders            │
│   KPI-002  Gross Margin %         KPI-007  Avg Discount Rate       │
│   KPI-003  Revenue Growth MoM     KPI-008  Revenue per Rep         │
│   KPI-004  Avg Order Value        KPI-009  Customer Count          │
│   KPI-005  Target Attainment %    KPI-010  Top 10 Revenue Share    │
│                                                                     │
│   Output: kpi_results.csv, kpi_monthly_trend.csv,                 │
│           top_products.csv, regional_performance.csv               │
└──────────────────────────────┬──────────────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────────────┐
│              SAP ANALYTICS CLOUD (SAC) STORY LAYER                 │
│                  dashboards/story_config.json                      │
│                                                                     │
│   Page 1: Executive Overview     KPI tiles, line chart, map        │
│   Page 2: Sales Trend Analysis   MoM/YoY trends, waterfall         │
│   Page 3: Regional Performance   Geo choropleth, bullet chart       │
│   Page 4: Product Analysis       Top N bar, scatter, treemap        │
└─────────────────────────────────────────────────────────────────────┘
```

---

## Data Flow

1. **Source Extraction** — Data extracted from ERP/CRM systems into raw CSV format.
2. **Staging** — Raw files land in `data/raw/` without transformation.
3. **ETL Transform** — `transform.py` cleans, casts, enriches, and validates all tables.
4. **Quality Gate** — `validate.py` runs 35+ automated checks; pipeline stops on failures.
5. **Star Schema** — Processed, validated tables written to `data/processed/`.
6. **KPI Engine** — `kpi_calculator.py` reads processed tables and computes all 10 KPIs with RAG status.
7. **SAC Ingestion** — Processed CSVs uploaded to SAC; story layout defined in `story_config.json`.

---

## Design Decisions

### Dimensional Modeling (Star Schema)
Chosen over 3NF for analytics performance. The denormalized star schema minimises JOIN complexity in SAC queries and supports natural drill-down hierarchies.

### Dimensional Hierarchies
Each dimension exposes at least one drill-down hierarchy to enable SAC's built-in hierarchy navigation:
- Date: Year → Quarter → Month → Day
- Product: Category → Sub-Category → Brand → SKU
- Customer: Segment → Industry → Customer
- Region: Region → Country → Sub-Region → City

### KPI Separation
KPI business logic is isolated in `kpi_calculator.py` and declared in `kpi_definitions.json`. This separation means KPI thresholds and formulas can be updated without changing application code.

### Validation-First Pipeline
The pipeline will not write processed data if critical validation checks fail. This prevents bad data from reaching the SAC model and corrupting dashboard metrics.

---

## Technology Stack

| Layer | Technology |
|-------|------------|
| Language | Python 3.10+ |
| Data Processing | pandas, numpy |
| Test Framework | pytest |
| Data Generation | Faker |
| Analytics Platform | SAP Analytics Cloud |
| Modeling Concept | Dimensional Modeling / Star Schema |
| Version Control | Git / GitHub |
