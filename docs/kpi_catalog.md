# KPI Catalog — SAC Analytics Dashboard

## KPI Governance

| Attribute | Detail |
|-----------|--------|
| Owner | Analytics Implementation Team |
| Review Cycle | Quarterly |
| Source of Truth | `kpi_definitions.json` |
| SAC Model | SAC_SalesAnalytics_Model |

---

## KPI Definitions

### KPI-001 | Total Revenue
- **Category:** Sales
- **Formula:** `SUM(sales_amount)` where `order_status != 'Cancelled'`
- **Unit:** USD (Currency)
- **RAG Thresholds:** 🟢 ≥ $10M | 🟡 ≥ $7M | 🔴 < $7M
- **Trend:** Higher is better
- **Description:** Aggregate revenue from all non-cancelled sales orders. Primary top-line performance metric.

---

### KPI-002 | Gross Margin Percentage
- **Category:** Profitability
- **Formula:** `SUM(gross_margin) / SUM(sales_amount) × 100`
- **Unit:** Percentage
- **RAG Thresholds:** 🟢 ≥ 55% | 🟡 ≥ 25% | 🔴 < 25%
- **Trend:** Higher is better
- **Description:** Percentage of revenue retained after deducting cost of goods sold. Key profitability indicator.

---

### KPI-003 | Revenue Growth MoM
- **Category:** Trend
- **Formula:** `(Current Month Revenue − Prior Month Revenue) / Prior Month Revenue × 100`
- **Unit:** Percentage
- **RAG Thresholds:** 🟢 ≥ 10% | 🟡 ≥ 0% | 🔴 < 0%
- **Trend:** Higher is better
- **Description:** Month-over-month revenue growth rate. Indicates sales momentum.

---

### KPI-004 | Average Order Value (AOV)
- **Category:** Sales
- **Formula:** `SUM(sales_amount) / COUNT(DISTINCT order_id)`
- **Unit:** USD (Currency)
- **RAG Thresholds:** 🟢 ≥ $5,000 | 🟡 ≥ $500 | 🔴 < $500
- **Trend:** Higher is better
- **Description:** Average revenue per distinct order. Measures deal size and up-sell effectiveness.

---

### KPI-005 | Sales Target Attainment
- **Category:** Performance
- **Formula:** `SUM(sales_amount) / SUM(target_amount) × 100`
- **Unit:** Percentage
- **RAG Thresholds:** 🟢 ≥ 100% | 🟡 ≥ 80% | 🔴 < 80%
- **Trend:** Higher is better
- **Description:** Percentage of sales target achieved. Core performance management KPI for reps and regions.

---

### KPI-006 | Total Orders
- **Category:** Sales
- **Formula:** `COUNT(DISTINCT order_id)`
- **Unit:** Count
- **RAG Thresholds:** 🟢 ≥ 2,000 | 🟡 ≥ 500 | 🔴 < 500
- **Trend:** Higher is better
- **Description:** Number of distinct orders placed. Volume indicator.

---

### KPI-007 | Average Discount Rate
- **Category:** Sales
- **Formula:** `AVG(discount_pct) × 100`
- **Unit:** Percentage
- **RAG Thresholds:** 🟢 ≤ 5% | 🟡 ≤ 20% | 🔴 > 20%
- **Trend:** Lower is better
- **Description:** Average discount applied across orders. High discount rates can signal pricing pressure or margin risk.

---

### KPI-008 | Revenue per Sales Rep
- **Category:** Productivity
- **Formula:** `SUM(sales_amount) / COUNT(DISTINCT employee_key)`
- **Unit:** USD (Currency)
- **RAG Thresholds:** 🟢 ≥ $500K | 🟡 ≥ $100K | 🔴 < $100K
- **Trend:** Higher is better
- **Description:** Revenue productivity per sales representative. Headcount efficiency metric.

---

### KPI-009 | Active Customer Count
- **Category:** Customer
- **Formula:** `COUNT(DISTINCT customer_key)`
- **Unit:** Count
- **RAG Thresholds:** 🟢 ≥ 200 | 🟡 ≥ 50 | 🔴 < 50
- **Trend:** Higher is better
- **Description:** Number of unique customers with at least one purchase in the period.

---

### KPI-010 | Top 10 Products Revenue Share
- **Category:** Product
- **Formula:** `SUM(Top 10 Products Revenue) / SUM(All Revenue) × 100`
- **Unit:** Percentage
- **Trend:** Informational
- **Description:** Revenue concentration in the top 10 products. High concentration may indicate product portfolio risk.
