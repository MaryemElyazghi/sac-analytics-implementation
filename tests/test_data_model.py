"""
test_data_model.py
------------------
Unit tests for star schema integrity, dimension completeness,
and referential integrity checks.
"""

import os
import sys
import pytest
import pandas as pd

PROCESSED_DIR = os.path.join(os.path.dirname(__file__), "..", "data", "processed")


def load(table: str) -> pd.DataFrame:
    path = os.path.join(PROCESSED_DIR, f"{table}.csv")
    if not os.path.exists(path):
        pytest.skip(f"Processed file not found: {table}.csv — run pipeline first.")
    return pd.read_csv(path)


# ─────────────────────────────────────────────
# DIM_DATE TESTS
# ─────────────────────────────────────────────
class TestDimDate:
    def setup_method(self):
        self.df = load("dim_date")

    def test_no_null_date_key(self):
        assert self.df["date_key"].isnull().sum() == 0

    def test_no_duplicate_date_key(self):
        assert self.df["date_key"].duplicated().sum() == 0

    def test_month_number_range(self):
        assert self.df["month_number"].between(1, 12).all()

    def test_day_of_month_range(self):
        assert self.df["day_of_month"].between(1, 31).all()

    def test_valid_quarter_values(self):
        valid = {"Q1", "Q2", "Q3", "Q4"}
        assert set(self.df["quarter"].unique()).issubset(valid)

    def test_date_key_format(self):
        """date_key should be 8-digit YYYYMMDD integer."""
        assert (self.df["date_key"].astype(str).str.len() == 8).all()

    def test_non_empty(self):
        assert len(self.df) > 0


# ─────────────────────────────────────────────
# DIM_PRODUCT TESTS
# ─────────────────────────────────────────────
class TestDimProduct:
    def setup_method(self):
        self.df = load("dim_product")

    def test_no_null_product_key(self):
        assert self.df["product_key"].isnull().sum() == 0

    def test_no_duplicate_product_key(self):
        assert self.df["product_key"].duplicated().sum() == 0

    def test_positive_unit_cost(self):
        assert (self.df["unit_cost"] > 0).all()

    def test_positive_list_price(self):
        assert (self.df["list_price"] > 0).all()

    def test_product_id_format(self):
        assert self.df["product_id"].str.startswith("PRD-").all()

    def test_no_null_category(self):
        assert self.df["category"].isnull().sum() == 0


# ─────────────────────────────────────────────
# DIM_CUSTOMER TESTS
# ─────────────────────────────────────────────
class TestDimCustomer:
    def setup_method(self):
        self.df = load("dim_customer")

    def test_no_null_customer_key(self):
        assert self.df["customer_key"].isnull().sum() == 0

    def test_no_duplicate_customer_key(self):
        assert self.df["customer_key"].duplicated().sum() == 0

    def test_valid_segments(self):
        valid = {"Enterprise", "Mid-Market", "SMB", "Startup"}
        assert set(self.df["segment"].unique()).issubset(valid)

    def test_customer_id_format(self):
        assert self.df["customer_id"].str.startswith("CUST-").all()

    def test_no_null_customer_name(self):
        assert self.df["customer_name"].isnull().sum() == 0


# ─────────────────────────────────────────────
# DIM_EMPLOYEE TESTS
# ─────────────────────────────────────────────
class TestDimEmployee:
    def setup_method(self):
        self.df = load("dim_employee")

    def test_no_null_employee_key(self):
        assert self.df["employee_key"].isnull().sum() == 0

    def test_no_duplicate_employee_key(self):
        assert self.df["employee_key"].duplicated().sum() == 0

    def test_employee_id_format(self):
        assert self.df["employee_id"].str.startswith("EMP-").all()


# ─────────────────────────────────────────────
# DIM_REGION TESTS
# ─────────────────────────────────────────────
class TestDimRegion:
    def setup_method(self):
        self.df = load("dim_region")

    def test_no_null_region_key(self):
        assert self.df["region_key"].isnull().sum() == 0

    def test_no_duplicate_region_key(self):
        assert self.df["region_key"].duplicated().sum() == 0

    def test_no_null_country(self):
        assert self.df["country"].isnull().sum() == 0


# ─────────────────────────────────────────────
# FACT_SALES TESTS
# ─────────────────────────────────────────────
class TestFactSales:
    def setup_method(self):
        self.fact = load("fact_sales")

    def test_no_null_sales_key(self):
        assert self.fact["sales_key"].isnull().sum() == 0

    def test_no_duplicate_sales_key(self):
        assert self.fact["sales_key"].duplicated().sum() == 0

    def test_positive_quantity(self):
        assert (self.fact["quantity"] > 0).all()

    def test_non_negative_sales_amount(self):
        assert (self.fact["sales_amount"] >= 0).all()

    def test_non_negative_cogs(self):
        assert (self.fact["cogs"] >= 0).all()

    def test_discount_pct_range(self):
        assert self.fact["discount_pct"].between(0, 1).all()

    def test_valid_order_statuses(self):
        valid = {"Open", "Confirmed", "Shipped", "Delivered", "Cancelled"}
        assert set(self.fact["order_status"].unique()).issubset(valid)

    def test_valid_channels(self):
        valid = {"Direct", "Partner", "Online", "Retail"}
        assert set(self.fact["channel"].unique()).issubset(valid)

    def test_fk_date_key(self):
        dim_date = load("dim_date")
        orphans = ~self.fact["date_key"].isin(dim_date["date_key"])
        assert orphans.sum() == 0, f"{orphans.sum()} orphan date_key values"

    def test_fk_product_key(self):
        dim_product = load("dim_product")
        orphans = ~self.fact["product_key"].isin(dim_product["product_key"])
        assert orphans.sum() == 0

    def test_fk_customer_key(self):
        dim_customer = load("dim_customer")
        orphans = ~self.fact["customer_key"].isin(dim_customer["customer_key"])
        assert orphans.sum() == 0

    def test_fk_employee_key(self):
        dim_employee = load("dim_employee")
        orphans = ~self.fact["employee_key"].isin(dim_employee["employee_key"])
        assert orphans.sum() == 0

    def test_fk_region_key(self):
        dim_region = load("dim_region")
        orphans = ~self.fact["region_key"].isin(dim_region["region_key"])
        assert orphans.sum() == 0

    def test_non_empty_fact(self):
        assert len(self.fact) > 0

    def test_revenue_eligible_flag_exists(self):
        assert "is_revenue_eligible" in self.fact.columns
