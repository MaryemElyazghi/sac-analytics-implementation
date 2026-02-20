# Data Dictionary — SAC Analytics Implementation

## Overview

This document describes all tables, columns, and business definitions used in the SAP Analytics Cloud analytics solution.

---

## Star Schema Overview

| Table | Type | Grain | Rows (approx) |
|-------|------|-------|---------------|
| `dim_date` | Dimension | One row per calendar day | ~730 |
| `dim_product` | Dimension | One row per product SKU | ~80 |
| `dim_customer` | Dimension | One row per customer account | ~300 |
| `dim_employee` | Dimension | One row per sales rep | ~40 |
| `dim_region` | Dimension | One row per city/location | ~20 |
| `fact_sales` | Fact | One row per order line item | ~30,000+ |

---

## dim_date

| Column | Type | Description | Example |
|--------|------|-------------|---------|
| `date_key` | INTEGER | Surrogate key (YYYYMMDD format) | `20250115` |
| `full_date` | DATE | Calendar date | `2025-01-15` |
| `day_of_week` | STRING | Name of the day | `Wednesday` |
| `day_of_month` | INTEGER | Day number within the month (1–31) | `15` |
| `week_number` | INTEGER | ISO week number (1–53) | `3` |
| `month_number` | INTEGER | Month number (1–12) | `1` |
| `month_name` | STRING | Full month name | `January` |
| `quarter` | STRING | Fiscal/calendar quarter | `Q1` |
| `year` | INTEGER | Calendar year | `2025` |
| `is_weekend` | BOOLEAN | True if Saturday or Sunday | `false` |
| `is_holiday` | BOOLEAN | True if a public holiday | `false` |
| `fiscal_period` | STRING | Fiscal year and period | `FY2025-P01` |

---

## dim_product

| Column | Type | Description | Example |
|--------|------|-------------|---------|
| `product_key` | INTEGER | Surrogate primary key | `1` |
| `product_id` | STRING | Business product code | `PRD-0001` |
| `product_name` | STRING | Full product name | `TechCorp Laptops AB-123` |
| `category` | STRING | Top-level product category | `Electronics` |
| `sub_category` | STRING | Product sub-category | `Laptops` |
| `brand` | STRING | Brand or vendor | `TechCorp` |
| `unit_cost` | DECIMAL(10,2) | Cost of goods per unit | `850.00` |
| `list_price` | DECIMAL(10,2) | Standard selling price | `1499.99` |
| `is_active` | BOOLEAN | Whether product is still sold | `true` |
| `launch_date` | DATE | Date product was introduced | `2023-03-01` |
| `margin_band` | STRING | Derived margin classification | `High (40-60%)` |

---

## dim_customer

| Column | Type | Description | Example |
|--------|------|-------------|---------|
| `customer_key` | INTEGER | Surrogate primary key | `1` |
| `customer_id` | STRING | Business customer code | `CUST-00001` |
| `customer_name` | STRING | Legal company name | `Acme Corporation` |
| `segment` | STRING | Sales segment | `Enterprise` |
| `industry` | STRING | Customer industry vertical | `Manufacturing` |
| `email` | STRING | Primary contact email | `contact@acme.com` |
| `acquisition_date` | DATE | Date customer first purchased | `2022-06-15` |
| `is_active` | BOOLEAN | Whether customer is active | `true` |

**Valid Segments:** Enterprise, Mid-Market, SMB, Startup

---

## dim_employee

| Column | Type | Description | Example |
|--------|------|-------------|---------|
| `employee_key` | INTEGER | Surrogate primary key | `1` |
| `employee_id` | STRING | HR employee code | `EMP-001` |
| `full_name` | STRING | Employee full name | `Jane Smith` |
| `department` | STRING | Department name | `Enterprise Sales` |
| `job_title` | STRING | Job title | `Account Executive` |
| `manager_id` | STRING | Employee ID of direct manager | `EMP-005` |
| `hire_date` | DATE | Date employee joined | `2021-09-01` |
| `region_key` | INTEGER | FK to dim_region | `3` |
| `is_active` | BOOLEAN | Whether employee is active | `true` |

---

## dim_region

| Column | Type | Description | Example |
|--------|------|-------------|---------|
| `region_key` | INTEGER | Surrogate primary key | `1` |
| `country` | STRING | Country name | `United States` |
| `region` | STRING | Macro region | `North America` |
| `sub_region` | STRING | Sub-region within country | `Northeast US` |
| `city` | STRING | Primary city | `New York` |
| `currency` | STRING | Local currency code (ISO 4217) | `USD` |

---

## fact_sales

| Column | Type | Description | Example |
|--------|------|-------------|---------|
| `sales_key` | INTEGER | Surrogate primary key | `1` |
| `order_id` | STRING | Business order identifier | `ORD-000001` |
| `line_number` | INTEGER | Line item within an order | `1` |
| `date_key` | INTEGER | FK → dim_date.date_key | `20250115` |
| `product_key` | INTEGER | FK → dim_product.product_key | `42` |
| `customer_key` | INTEGER | FK → dim_customer.customer_key | `18` |
| `region_key` | INTEGER | FK → dim_region.region_key | `3` |
| `employee_key` | INTEGER | FK → dim_employee.employee_key | `7` |
| `quantity` | INTEGER | Units sold | `5` |
| `unit_price` | DECIMAL(10,2) | Actual selling price per unit | `1350.00` |
| `discount_pct` | DECIMAL(5,2) | Discount applied (0.00–1.00) | `0.10` |
| `sales_amount` | DECIMAL(14,2) | Revenue = qty × unit_price × (1-discount) | `6075.00` |
| `cogs` | DECIMAL(14,2) | Total cost of goods sold | `4250.00` |
| `gross_margin` | DECIMAL(14,2) | sales_amount − cogs | `1825.00` |
| `target_amount` | DECIMAL(14,2) | Sales target for this record | `6000.00` |
| `order_status` | STRING | Current order status | `Delivered` |
| `channel` | STRING | Sales channel | `Direct` |
| `is_revenue_eligible` | BOOLEAN | False for Cancelled orders | `true` |
| `gross_margin_pct` | DECIMAL(5,2) | Derived: gross_margin / sales_amount × 100 | `30.04` |
| `target_attainment_pct` | DECIMAL(5,2) | Derived: sales_amount / target_amount × 100 | `101.25` |
| `discount_impact` | DECIMAL(14,2) | Derived: revenue foregone due to discount | `675.00` |
| `created_at` | TIMESTAMP | Record creation timestamp | `2025-01-15 09:23:11` |
| `updated_at` | TIMESTAMP | Record last update timestamp | `2025-01-15 09:23:11` |

**Valid Order Statuses:** Open, Confirmed, Shipped, Delivered, Cancelled

**Valid Channels:** Direct, Partner, Online, Retail
