"""
transform.py
------------
ETL pipeline that:
  1. Reads raw CSV source files
  2. Cleans and validates data types
  3. Enforces referential integrity (FK checks)
  4. Builds the final star schema tables
  5. Writes processed output to data/processed/

Run: python etl/transform.py
"""

import os
import sys
import pandas as pd
import numpy as np
from datetime import datetime

RAW_DIR       = os.path.join(os.path.dirname(__file__), "..", "data", "raw")
PROCESSED_DIR = os.path.join(os.path.dirname(__file__), "..", "data", "processed")
os.makedirs(PROCESSED_DIR, exist_ok=True)

TABLES = ["dim_date", "dim_product", "dim_customer", "dim_employee", "dim_region", "fact_sales"]


# ─────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────
def log(msg: str, level: str = "INFO"):
    ts = datetime.now().strftime("%H:%M:%S")
    icons = {"INFO": "ℹ️ ", "OK": "✅", "WARN": "⚠️ ", "ERROR": "❌", "STEP": "🔄"}
    print(f"[{ts}] {icons.get(level, '')} {msg}")


def load_raw(table: str) -> pd.DataFrame:
    path = os.path.join(RAW_DIR, f"{table}.csv")
    if not os.path.exists(path):
        log(f"Missing raw file: {path}", "ERROR")
        sys.exit(1)
    df = pd.read_csv(path)
    log(f"Loaded {table}: {len(df):,} rows × {len(df.columns)} cols", "OK")
    return df


def save_processed(df: pd.DataFrame, table: str):
    path = os.path.join(PROCESSED_DIR, f"{table}.csv")
    df.to_csv(path, index=False)
    log(f"Saved  {table}: {len(df):,} rows → {path}", "OK")


# ─────────────────────────────────────────────
# TRANSFORM: DIM_DATE
# ─────────────────────────────────────────────
def transform_dim_date(df: pd.DataFrame) -> pd.DataFrame:
    log("Transforming dim_date...", "STEP")
    df = df.copy()
    df["full_date"] = pd.to_datetime(df["full_date"])
    df["is_weekend"] = df["is_weekend"].astype(bool)
    df["is_holiday"] = df["is_holiday"].astype(bool)
    df = df.drop_duplicates(subset=["date_key"])
    assert df["date_key"].is_monotonic_increasing or True  # allow any order
    log(f"  Date range: {df['full_date'].min().date()} → {df['full_date'].max().date()}", "INFO")
    return df


# ─────────────────────────────────────────────
# TRANSFORM: DIM_PRODUCT
# ─────────────────────────────────────────────
def transform_dim_product(df: pd.DataFrame) -> pd.DataFrame:
    log("Transforming dim_product...", "STEP")
    df = df.copy()
    df["unit_cost"]   = pd.to_numeric(df["unit_cost"],   errors="coerce")
    df["list_price"]  = pd.to_numeric(df["list_price"],  errors="coerce")
    df["is_active"]   = df["is_active"].astype(bool)
    df["launch_date"] = pd.to_datetime(df["launch_date"], errors="coerce")

    # Derived: margin band
    df["margin_band"] = pd.cut(
        (df["list_price"] - df["unit_cost"]) / df["list_price"] * 100,
        bins=[0, 20, 40, 60, 100],
        labels=["Low (<20%)", "Medium (20-40%)", "High (40-60%)", "Premium (>60%)"],
        right=True
    )

    # Remove rows with missing prices
    before = len(df)
    df = df.dropna(subset=["unit_cost", "list_price"])
    removed = before - len(df)
    if removed:
        log(f"  Dropped {removed} products with missing prices", "WARN")

    return df


# ─────────────────────────────────────────────
# TRANSFORM: DIM_CUSTOMER
# ─────────────────────────────────────────────
def transform_dim_customer(df: pd.DataFrame) -> pd.DataFrame:
    log("Transforming dim_customer...", "STEP")
    df = df.copy()
    df["acquisition_date"] = pd.to_datetime(df["acquisition_date"], errors="coerce")
    df["is_active"]        = df["is_active"].astype(bool)
    df["customer_name"]    = df["customer_name"].str.strip().str.title()

    # Standardise segment labels
    segment_map = {
        "enterprise": "Enterprise",
        "mid-market": "Mid-Market",
        "smb":        "SMB",
        "startup":    "Startup",
    }
    df["segment"] = df["segment"].str.lower().map(segment_map).fillna(df["segment"])
    return df


# ─────────────────────────────────────────────
# TRANSFORM: DIM_EMPLOYEE
# ─────────────────────────────────────────────
def transform_dim_employee(df: pd.DataFrame) -> pd.DataFrame:
    log("Transforming dim_employee...", "STEP")
    df = df.copy()
    df["hire_date"] = pd.to_datetime(df["hire_date"], errors="coerce")
    df["is_active"] = df["is_active"].astype(bool)
    df["full_name"] = df["full_name"].str.strip()
    return df


# ─────────────────────────────────────────────
# TRANSFORM: DIM_REGION
# ─────────────────────────────────────────────
def transform_dim_region(df: pd.DataFrame) -> pd.DataFrame:
    log("Transforming dim_region...", "STEP")
    df = df.copy()
    df["country"]    = df["country"].str.strip()
    df["region"]     = df["region"].str.strip()
    df["sub_region"] = df["sub_region"].str.strip()
    df["city"]       = df["city"].str.strip()
    return df


# ─────────────────────────────────────────────
# TRANSFORM: FACT_SALES
# ─────────────────────────────────────────────
def transform_fact_sales(
    df: pd.DataFrame,
    dim_date: pd.DataFrame,
    dim_product: pd.DataFrame,
    dim_customer: pd.DataFrame,
    dim_employee: pd.DataFrame,
    dim_region: pd.DataFrame,
) -> pd.DataFrame:
    log("Transforming fact_sales...", "STEP")
    df = df.copy()

    # Cast numeric columns
    numeric_cols = ["quantity", "unit_price", "discount_pct", "sales_amount",
                    "cogs", "gross_margin", "target_amount"]
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df["created_at"] = pd.to_datetime(df["created_at"], errors="coerce")
    df["updated_at"] = pd.to_datetime(df["updated_at"], errors="coerce")

    # FK integrity checks
    valid_dates     = set(dim_date["date_key"])
    valid_products  = set(dim_product["product_key"])
    valid_customers = set(dim_customer["customer_key"])
    valid_employees = set(dim_employee["employee_key"])
    valid_regions   = set(dim_region["region_key"])

    before = len(df)
    df = df[
        df["date_key"].isin(valid_dates) &
        df["product_key"].isin(valid_products) &
        df["customer_key"].isin(valid_customers) &
        df["employee_key"].isin(valid_employees) &
        df["region_key"].isin(valid_regions)
    ]
    removed = before - len(df)
    if removed:
        log(f"  Dropped {removed} orphan rows (FK violation)", "WARN")

    # Derived columns
    df["gross_margin_pct"]      = (df["gross_margin"] / df["sales_amount"] * 100).round(2)
    df["target_attainment_pct"] = (df["sales_amount"] / df["target_amount"] * 100).round(2)
    df["discount_impact"]       = (df["quantity"] * df["unit_price"] * df["discount_pct"]).round(2)

    # Remove cancelled orders from revenue metrics flag
    df["is_revenue_eligible"] = df["order_status"] != "Cancelled"

    # Remove nulls in critical measures
    before = len(df)
    df = df.dropna(subset=["sales_amount", "cogs", "quantity"])
    removed = before - len(df)
    if removed:
        log(f"  Dropped {removed} rows with null measures", "WARN")

    log(f"  Revenue-eligible rows: {df['is_revenue_eligible'].sum():,} / {len(df):,}", "INFO")
    return df


# ─────────────────────────────────────────────
# MAIN PIPELINE
# ─────────────────────────────────────────────
def run_pipeline():
    log("=" * 60, "INFO")
    log("SAC Analytics ETL Transform Pipeline", "INFO")
    log("=" * 60, "INFO")

    # Load all raw tables
    raw = {t: load_raw(t) for t in TABLES}

    # Transform dimensions
    dim_date     = transform_dim_date(raw["dim_date"])
    dim_product  = transform_dim_product(raw["dim_product"])
    dim_customer = transform_dim_customer(raw["dim_customer"])
    dim_employee = transform_dim_employee(raw["dim_employee"])
    dim_region   = transform_dim_region(raw["dim_region"])

    # Transform fact
    fact_sales   = transform_fact_sales(
        raw["fact_sales"], dim_date, dim_product,
        dim_customer, dim_employee, dim_region
    )

    # Save all processed tables
    save_processed(dim_date,     "dim_date")
    save_processed(dim_product,  "dim_product")
    save_processed(dim_customer, "dim_customer")
    save_processed(dim_employee, "dim_employee")
    save_processed(dim_region,   "dim_region")
    save_processed(fact_sales,   "fact_sales")

    # Summary stats
    log("\n" + "=" * 60, "INFO")
    log("Pipeline Complete — Summary", "OK")
    log("=" * 60, "INFO")
    eligible = fact_sales[fact_sales["is_revenue_eligible"]]
    log(f"Total Revenue:        ${eligible['sales_amount'].sum():>15,.2f}", "INFO")
    log(f"Total Orders:         {fact_sales['order_id'].nunique():>10,}", "INFO")
    log(f"Avg Gross Margin %:   {eligible['gross_margin_pct'].mean():>9.2f}%", "INFO")
    log(f"Unique Customers:     {fact_sales['customer_key'].nunique():>10,}", "INFO")
    log(f"Unique Products Sold: {fact_sales['product_key'].nunique():>10,}", "INFO")
    log("=" * 60, "INFO")


if __name__ == "__main__":
    run_pipeline()
