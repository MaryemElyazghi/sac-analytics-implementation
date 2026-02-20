# SAP Analytics Cloud (SAC) Analytics Implementation

> End-to-end analytics implementation project covering data modeling, KPI dashboards, business reporting, and functional validation — built for SAP Analytics Cloud (Jan–Feb 2026).

---

## 📁 Project Structure

```
sac-analytics-implementation/
├── data/
│   ├── raw/                    # Source CSV files (simulated ERP data)
│   └── processed/              # Cleaned/transformed output
├── models/
│   ├── dimensions/             # Dimension table schemas (JSON)
│   └── facts/                  # Fact table schemas (JSON)
├── etl/
│   ├── generate_sample_data.py # Synthetic ERP data generator
│   ├── transform.py            # ETL pipeline (clean, join, enrich)
│   └── validate.py             # Data quality validation engine
├── kpis/
│   ├── kpi_definitions.json    # KPI catalog with formulas & thresholds
│   └── kpi_calculator.py       # KPI computation engine
├── dashboards/
│   └── story_config.json       # SAC Story layout & widget configuration
├── tests/
│   ├── test_data_model.py      # Unit tests for schema integrity
│   ├── test_kpis.py            # Unit tests for KPI calculations
│   └── test_validation.py      # End-to-end validation tests
├── docs/
│   ├── data_dictionary.md      # Full data dictionary
│   ├── kpi_catalog.md          # Business KPI definitions
│   └── architecture.md         # Solution architecture overview
├── scripts/
│   └── run_pipeline.sh         # Full pipeline execution script
├── requirements.txt
├── .gitignore
└── README.md
```

---

## 🏗️ Architecture Overview

```
[Source Systems (ERP/CRM)]
         │
         ▼
[ETL Pipeline (Python)]
  ├── generate_sample_data.py
  ├── transform.py
  └── validate.py
         │
         ▼
[Star Schema Data Model]
  ├── dim_date
  ├── dim_product
  ├── dim_customer
  ├── dim_region
  ├── dim_employee
  └── fact_sales
         │
         ▼
[KPI Calculation Engine]
  └── kpi_calculator.py
         │
         ▼
[SAC Story / Dashboard Layer]
  ├── Executive KPI Dashboard
  ├── Sales Trend Analysis
  ├── Regional Performance
  └── Product Drill-Down
```

---

## ⚡ Quick Start

### 1. Clone the Repository
```bash
git clone https://github.com/YOUR_USERNAME/sac-analytics-implementation.git
cd sac-analytics-implementation
```

### 2. Set Up Environment
```bash
python -m venv venv
source venv/bin/activate        # Windows: venv\Scripts\activate
pip install -r requirements.txt
```

### 3. Run the Full Pipeline
```bash
bash scripts/run_pipeline.sh
```

Or step by step:
```bash
# Step 1: Generate sample ERP data
python etl/generate_sample_data.py

# Step 2: Transform and build star schema
python etl/transform.py

# Step 3: Validate data quality
python etl/validate.py

# Step 4: Calculate KPIs
python kpis/kpi_calculator.py

# Step 5: Run all tests
pytest tests/ -v
```

---

## 📊 KPIs Implemented

| KPI | Category | Formula |
|-----|----------|---------|
| Total Revenue | Sales | SUM(sales_amount) |
| Gross Margin % | Profitability | (Revenue - COGS) / Revenue × 100 |
| Revenue Growth MoM | Trend | (Current Month - Prior Month) / Prior Month × 100 |
| Top N Products | Product | TOPN(product, revenue, N) |
| Customer Acquisition Rate | Customer | New Customers / Total Customers × 100 |
| Average Order Value (AOV) | Sales | Total Revenue / Order Count |
| Sales Target Attainment | Performance | Actual Revenue / Target Revenue × 100 |
| Regional Revenue Share | Geography | Region Revenue / Total Revenue × 100 |
| Inventory Turnover | Operations | COGS / Average Inventory |
| Employee Sales Productivity | HR | Revenue / Headcount |

---

## 🧪 Testing

```bash
pytest tests/ -v --tb=short
```

Test coverage includes:
- Star schema referential integrity
- Null / duplicate checks on dimension keys
- KPI formula accuracy vs expected values
- Threshold alert validation
- End-to-end pipeline output verification

---

## 🗂️ Data Model

### Star Schema

```
                    ┌─────────────────┐
                    │   dim_date      │
                    │  date_key (PK)  │
                    └────────┬────────┘
                             │
┌──────────────┐    ┌────────▼─────────┐    ┌───────────────┐
│ dim_product  │    │   fact_sales     │    │ dim_customer  │
│ product_key  ├────┤  date_key (FK)   ├────┤ customer_key  │
│ (PK)         │    │  product_key(FK) │    │ (PK)          │
└──────────────┘    │  customer_key(FK)│    └───────────────┘
                    │  region_key (FK) │
┌──────────────┐    │  employee_key(FK)│    ┌───────────────┐
│  dim_region  ├────┤  sales_amount    ├────┤ dim_employee  │
│  region_key  │    │  quantity        │    │ employee_key  │
│  (PK)        │    │  cogs            │    │ (PK)          │
└──────────────┘    │  target_amount   │    └───────────────┘
                    └──────────────────┘
```

---

## 📄 License

MIT License — see [LICENSE](LICENSE) for details.

---

## 👤 Author

Built as part of SAP Analytics Cloud Analytics Implementation Project (Jan–Feb 2026).
