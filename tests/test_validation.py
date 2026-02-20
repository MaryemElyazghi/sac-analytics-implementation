"""
test_validation.py
------------------
End-to-end validation tests that run after the full pipeline.
Asserts processed data meets quality thresholds for SAC ingestion.
"""

import os
import sys
import pytest
import pandas as pd

PROCESSED_DIR = os.path.join(os.path.dirname(__file__), "..", "data", "processed")


def has_processed_data() -> bool:
    return os.path.exists(os.path.join(PROCESSED_DIR, "fact_sales.csv"))


def load(table: str) -> pd.DataFrame:
    path = os.path.join(PROCESSED_DIR, f"{table}.csv")
    if not os.path.exists(path):
        pytest.skip(f"File not found: {table}.csv — run pipeline first.")
    return pd.read_csv(path)


@pytest.mark.skipif(not has_processed_data(), reason="Processed data not found")
class TestEndToEndValidation:

    def test_all_processed_files_exist(self):
        tables = ["dim_date", "dim_product", "dim_customer",
                  "dim_employee", "dim_region", "fact_sales"]
        for t in tables:
            assert os.path.exists(os.path.join(PROCESSED_DIR, f"{t}.csv")), \
                f"Missing processed file: {t}.csv"

    def test_fact_sales_row_count(self):
        """Must have meaningful data — not just a few rows."""
        df = load("fact_sales")
        assert len(df) >= 1000, f"fact_sales has only {len(df)} rows — expected >= 1000"

    def test_null_rate_below_threshold(self):
        """No critical column should exceed 1% nulls."""
        df = load("fact_sales")
        critical_cols = ["sales_key", "order_id", "date_key", "product_key",
                         "customer_key", "region_key", "employee_key",
                         "sales_amount", "quantity"]
        for col in critical_cols:
            null_rate = df[col].isnull().mean()
            assert null_rate < 0.01, \
                f"Column '{col}' has {null_rate:.1%} nulls (threshold: 1%)"

    def test_cancelled_orders_excluded_from_revenue_flag(self):
        df = load("fact_sales")
        cancelled = df[df["order_status"] == "Cancelled"]
        if len(cancelled) > 0:
            # is_revenue_eligible should be False for cancelled
            ineligible = cancelled["is_revenue_eligible"]
            assert not ineligible.any(), \
                "Some cancelled orders have is_revenue_eligible=True"

    def test_date_dimension_coverage(self):
        """All date_keys in fact must exist in dim_date."""
        fact = load("fact_sales")
        dim  = load("dim_date")
        missing = ~fact["date_key"].isin(dim["date_key"])
        assert missing.sum() == 0, \
            f"{missing.sum()} fact rows have no matching date in dim_date"

    def test_revenue_greater_than_zero(self):
        df = load("fact_sales")
        eligible = df[df["is_revenue_eligible"] == True]
        assert eligible["sales_amount"].sum() > 0

    def test_gross_margin_consistency(self):
        df = load("fact_sales")
        # gross_margin should equal sales_amount - cogs (within float tolerance)
        calc_gm = (df["sales_amount"] - df["cogs"]).round(2)
        match = (calc_gm - df["gross_margin"].round(2)).abs() < 1.0  # $1 tolerance
        assert match.mean() > 0.99, \
            f"gross_margin inconsistent with sales_amount - cogs in {(~match).sum()} rows"

    def test_kpi_results_file_exists(self):
        """kpi_results.csv must exist after calculator run."""
        path = os.path.join(PROCESSED_DIR, "kpi_results.csv")
        if not os.path.exists(path):
            pytest.skip("kpi_results.csv not found — run kpi_calculator.py first.")
        df = pd.read_csv(path)
        assert len(df) == 10, f"Expected 10 KPI results, got {len(df)}"
        assert "rag_status" in df.columns
        assert "value" in df.columns

    def test_monthly_trend_file_exists(self):
        path = os.path.join(PROCESSED_DIR, "kpi_monthly_trend.csv")
        if not os.path.exists(path):
            pytest.skip("kpi_monthly_trend.csv not found — run kpi_calculator.py first.")
        df = pd.read_csv(path)
        assert len(df) >= 12, "Expected at least 12 months of trend data"

    def test_top_products_file_exists(self):
        path = os.path.join(PROCESSED_DIR, "top_products.csv")
        if not os.path.exists(path):
            pytest.skip("top_products.csv not found — run kpi_calculator.py first.")
        df = pd.read_csv(path)
        assert len(df) > 0

    def test_regional_performance_file_exists(self):
        path = os.path.join(PROCESSED_DIR, "regional_performance.csv")
        if not os.path.exists(path):
            pytest.skip("regional_performance.csv not found.")
        df = pd.read_csv(path)
        assert "revenue_share_pct" in df.columns
        assert df["revenue_share_pct"].sum() == pytest.approx(100.0, abs=1.0)
