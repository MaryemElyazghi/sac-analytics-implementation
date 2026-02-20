"""
test_kpis.py
------------
Unit tests for KPI calculation accuracy.
Uses known test datasets to verify formulas produce expected values.
"""

import os
import sys
import pytest
import pandas as pd
import numpy as np

# Ensure project root is in path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

PROCESSED_DIR = os.path.join(os.path.dirname(__file__), "..", "data", "processed")


def has_processed_data() -> bool:
    return os.path.exists(os.path.join(PROCESSED_DIR, "fact_sales.csv"))


# ─────────────────────────────────────────────
# FORMULA UNIT TESTS (no I/O — pure logic)
# ─────────────────────────────────────────────
class TestKPIFormulas:
    """Test KPI formula logic with controlled in-memory DataFrames."""

    def make_sales(self, rows: list) -> pd.DataFrame:
        columns = ["order_id", "sales_amount", "gross_margin", "cogs",
                   "target_amount", "discount_pct", "quantity",
                   "unit_price", "employee_key", "customer_key",
                   "order_status", "year", "month_number"]
        return pd.DataFrame(rows, columns=columns)

    def test_total_revenue(self):
        df = self.make_sales([
            ["ORD-001", 1000.00, 400.00, 600.00, 950.00, 0.10, 2, 550.00, 1, 1, "Delivered", 2025, 1],
            ["ORD-002", 2500.00, 1000.00, 1500.00, 2400.00, 0.05, 5, 525.00, 2, 2, "Delivered", 2025, 1],
            ["ORD-003", 500.00,  200.00, 300.00, 480.00, 0.0,  1, 500.00, 1, 3, "Cancelled", 2025, 1],
        ])
        eligible = df[df["order_status"] != "Cancelled"]
        assert eligible["sales_amount"].sum() == pytest.approx(3500.00)

    def test_gross_margin_pct(self):
        df = self.make_sales([
            ["ORD-001", 1000.00, 400.00, 600.00, 950.00, 0.10, 2, 550.00, 1, 1, "Delivered", 2025, 1],
            ["ORD-002", 2000.00, 1000.00, 1000.00, 2000.00, 0.00, 4, 500.00, 2, 2, "Shipped", 2025, 1],
        ])
        total_rev = df["sales_amount"].sum()      # 3000
        total_gm  = df["gross_margin"].sum()      # 1400
        gm_pct    = total_gm / total_rev * 100
        assert gm_pct == pytest.approx(46.6667, rel=1e-3)

    def test_avg_order_value(self):
        df = self.make_sales([
            ["ORD-001", 1000.00, 400.00, 600.00, 950.00, 0.10, 2, 550.00, 1, 1, "Delivered", 2025, 1],
            ["ORD-001", 500.00,  200.00, 300.00, 480.00, 0.05, 1, 500.00, 1, 1, "Delivered", 2025, 1],
            ["ORD-002", 2000.00, 800.00, 1200.00, 2000.00, 0.00, 4, 500.00, 2, 2, "Shipped", 2025, 1],
        ])
        total_rev   = df["sales_amount"].sum()         # 3500
        order_count = df["order_id"].nunique()         # 2
        aov = total_rev / order_count
        assert aov == pytest.approx(1750.00)

    def test_target_attainment(self):
        df = self.make_sales([
            ["ORD-001", 1050.00, 420.00, 630.00, 1000.00, 0.05, 2, 550.00, 1, 1, "Delivered", 2025, 1],
            ["ORD-002", 900.00,  360.00, 540.00, 1000.00, 0.10, 2, 500.00, 2, 2, "Shipped", 2025, 1],
        ])
        actual = df["sales_amount"].sum()  # 1950
        target = df["target_amount"].sum() # 2000
        attainment = actual / target * 100
        assert attainment == pytest.approx(97.5)

    def test_revenue_growth_mom(self):
        monthly = pd.DataFrame({
            "year":         [2025, 2025, 2025],
            "month_number": [1, 2, 3],
            "revenue":      [100000, 110000, 105000],
        })
        monthly["revenue_growth_mom"] = monthly["revenue"].pct_change() * 100
        assert monthly.iloc[1]["revenue_growth_mom"] == pytest.approx(10.0)
        assert monthly.iloc[2]["revenue_growth_mom"] == pytest.approx(-4.545, rel=1e-2)

    def test_discount_rate(self):
        df = self.make_sales([
            ["ORD-001", 950.00, 380.00, 570.00, 1000.00, 0.05, 2, 500.00, 1, 1, "Delivered", 2025, 1],
            ["ORD-002", 800.00, 320.00, 480.00, 1000.00, 0.20, 2, 500.00, 2, 2, "Delivered", 2025, 1],
            ["ORD-003", 1000.00, 400.00, 600.00, 1000.00, 0.00, 2, 500.00, 3, 3, "Delivered", 2025, 1],
        ])
        avg_discount = df["discount_pct"].mean() * 100
        assert avg_discount == pytest.approx(8.333, rel=1e-2)

    def test_revenue_per_employee(self):
        df = self.make_sales([
            ["ORD-001", 1000.00, 400.00, 600.00, 950.00, 0.00, 2, 500.00, 1, 1, "Delivered", 2025, 1],
            ["ORD-002", 2000.00, 800.00, 1200.00, 1900.00, 0.05, 4, 500.00, 1, 2, "Delivered", 2025, 1],
            ["ORD-003", 3000.00, 1200.00, 1800.00, 2900.00, 0.10, 6, 500.00, 2, 3, "Delivered", 2025, 1],
        ])
        total_rev  = df["sales_amount"].sum()       # 6000
        emp_count  = df["employee_key"].nunique()   # 2
        rev_per_emp = total_rev / emp_count
        assert rev_per_emp == pytest.approx(3000.00)

    def test_rag_status_green(self):
        """GREEN threshold: revenue >= 10,000,000"""
        from kpis.kpi_calculator import rag_status
        result = rag_status(12_000_000, {
            "excellent": {"operator": ">=", "value": 10_000_000},
            "good":      {"operator": ">=", "value": 7_000_000},
            "warning":   {"operator": ">=", "value": 4_000_000},
            "critical":  {"operator": "<",  "value": 4_000_000},
        }, "higher_is_better")
        assert result == "GREEN"

    def test_rag_status_amber(self):
        from kpis.kpi_calculator import rag_status
        result = rag_status(5_000_000, {
            "excellent": {"operator": ">=", "value": 10_000_000},
            "good":      {"operator": ">=", "value": 7_000_000},
            "warning":   {"operator": ">=", "value": 4_000_000},
            "critical":  {"operator": "<",  "value": 4_000_000},
        }, "higher_is_better")
        assert result == "AMBER"

    def test_rag_status_red(self):
        from kpis.kpi_calculator import rag_status
        result = rag_status(2_000_000, {
            "excellent": {"operator": ">=", "value": 10_000_000},
            "good":      {"operator": ">=", "value": 7_000_000},
            "warning":   {"operator": ">=", "value": 4_000_000},
            "critical":  {"operator": "<",  "value": 4_000_000},
        }, "higher_is_better")
        assert result == "RED"


# ─────────────────────────────────────────────
# INTEGRATION TESTS (requires processed data)
# ─────────────────────────────────────────────
@pytest.mark.skipif(not has_processed_data(), reason="Processed data not found — run pipeline first")
class TestKPIIntegration:
    def setup_method(self):
        sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
        from kpis.kpi_calculator import KPICalculator
        self.calc = KPICalculator()

    def test_total_revenue_positive(self):
        rev = self.calc.kpi_total_revenue()
        assert rev > 0, "Total revenue must be positive"

    def test_gross_margin_pct_in_range(self):
        gm = self.calc.kpi_gross_margin_pct()
        assert 0 <= gm <= 100, f"Gross margin % {gm:.2f} out of range"

    def test_avg_order_value_positive(self):
        aov = self.calc.kpi_avg_order_value()
        assert aov > 0

    def test_target_attainment_positive(self):
        ta = self.calc.kpi_target_attainment()
        assert ta > 0

    def test_discount_rate_in_range(self):
        dr = self.calc.kpi_discount_rate()
        assert 0 <= dr <= 100

    def test_monthly_trend_returns_dataframe(self):
        trend = self.calc.monthly_trend()
        assert isinstance(trend, pd.DataFrame)
        assert len(trend) > 0
        assert "revenue" in trend.columns

    def test_top_products_count(self):
        top = self.calc.top_products(10)
        assert len(top) <= 10

    def test_all_kpis_calculated(self):
        results = self.calc.calculate_all()
        assert len(results) == 10
        for r in results:
            assert "kpi_id" in r
            assert "value" in r
            assert "rag_status" in r
