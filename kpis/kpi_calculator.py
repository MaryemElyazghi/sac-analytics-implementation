"""
kpi_calculator.py
-----------------
Computes all KPIs defined in kpi_definitions.json against
the processed star schema tables.

Outputs:
  - Console summary with RAG (Red/Amber/Green) status
  - data/processed/kpi_results.csv
  - data/processed/kpi_monthly_trend.csv

Run: python kpis/kpi_calculator.py
"""

import os
import json
import pandas as pd
import numpy as np
from datetime import datetime

PROCESSED_DIR = os.path.join(os.path.dirname(__file__), "..", "data", "processed")
KPI_DEF_PATH  = os.path.join(os.path.dirname(__file__), "kpi_definitions.json")

# ANSI colour codes for terminal
GREEN  = "\033[92m"
YELLOW = "\033[93m"
RED    = "\033[91m"
CYAN   = "\033[96m"
BOLD   = "\033[1m"
RESET  = "\033[0m"


# ─────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────
def load(table: str) -> pd.DataFrame:
    return pd.read_csv(os.path.join(PROCESSED_DIR, f"{table}.csv"))


def rag_status(value: float, thresholds: dict, direction: str) -> str:
    """Return RAG label based on thresholds."""
    if not thresholds:
        return "INFO"
    t = thresholds
    def check(op, threshold):
        ops = {
            ">=": value >= threshold,
            "<=": value <= threshold,
            ">":  value > threshold,
            "<":  value < threshold,
            "=":  value == threshold,
        }
        return ops.get(op, False)

    if check(t["excellent"]["operator"], t["excellent"]["value"]):
        return "GREEN"
    if check(t["good"]["operator"], t["good"]["value"]):
        return "GREEN"
    if check(t["warning"]["operator"], t["warning"]["value"]):
        return "AMBER"
    return "RED"


def format_value(value: float, unit: str) -> str:
    if unit == "currency":
        return f"${value:,.2f}"
    elif unit == "percentage":
        return f"{value:.2f}%"
    elif unit == "count":
        return f"{value:,.0f}"
    return f"{value:.4f}"


def colour_rag(status: str) -> str:
    mapping = {"GREEN": GREEN, "AMBER": YELLOW, "RED": RED, "INFO": CYAN}
    return mapping.get(status, RESET)


# ─────────────────────────────────────────────
# KPI CALCULATIONS
# ─────────────────────────────────────────────
class KPICalculator:
    def __init__(self):
        self.fact    = load("fact_sales")
        self.dim_date= load("dim_date")
        with open(KPI_DEF_PATH) as f:
            self.kpi_defs = json.load(f)["kpi_catalog"]["kpis"]

        # Revenue eligible only
        self.eligible = self.fact[self.fact["is_revenue_eligible"] == True].copy()
        # Enrich with date info
        self.eligible = self.eligible.merge(
            self.dim_date[["date_key", "year", "month_number", "month_name", "quarter"]],
            on="date_key", how="left"
        )
        self.results = []

    # ── Core KPI methods ──────────────────────
    def kpi_total_revenue(self) -> float:
        return self.eligible["sales_amount"].sum()

    def kpi_gross_margin_pct(self) -> float:
        total_rev = self.eligible["sales_amount"].sum()
        total_gm  = self.eligible["gross_margin"].sum()
        return (total_gm / total_rev * 100) if total_rev > 0 else 0.0

    def kpi_revenue_growth_mom(self) -> float:
        monthly = (
            self.eligible.groupby(["year", "month_number"])["sales_amount"]
            .sum()
            .reset_index()
            .sort_values(["year", "month_number"])
        )
        if len(monthly) < 2:
            return 0.0
        cur  = monthly.iloc[-1]["sales_amount"]
        prev = monthly.iloc[-2]["sales_amount"]
        return ((cur - prev) / prev * 100) if prev > 0 else 0.0

    def kpi_avg_order_value(self) -> float:
        total_rev    = self.eligible["sales_amount"].sum()
        order_count  = self.eligible["order_id"].nunique()
        return (total_rev / order_count) if order_count > 0 else 0.0

    def kpi_target_attainment(self) -> float:
        actual = self.eligible["sales_amount"].sum()
        target = self.eligible["target_amount"].sum()
        return (actual / target * 100) if target > 0 else 0.0

    def kpi_total_orders(self) -> float:
        return float(self.eligible["order_id"].nunique())

    def kpi_discount_rate(self) -> float:
        return self.eligible["discount_pct"].mean() * 100

    def kpi_revenue_per_employee(self) -> float:
        total_rev  = self.eligible["sales_amount"].sum()
        emp_count  = self.eligible["employee_key"].nunique()
        return (total_rev / emp_count) if emp_count > 0 else 0.0

    def kpi_customer_count(self) -> float:
        return float(self.eligible["customer_key"].nunique())

    def kpi_top10_revenue_share(self) -> float:
        by_product   = self.eligible.groupby("product_key")["sales_amount"].sum().sort_values(ascending=False)
        total_rev    = by_product.sum()
        top10_rev    = by_product.head(10).sum()
        return (top10_rev / total_rev * 100) if total_rev > 0 else 0.0

    # ── KPI dispatcher ────────────────────────
    KPI_MAP = {
        "KPI-001": kpi_total_revenue,
        "KPI-002": kpi_gross_margin_pct,
        "KPI-003": kpi_revenue_growth_mom,
        "KPI-004": kpi_avg_order_value,
        "KPI-005": kpi_target_attainment,
        "KPI-006": kpi_total_orders,
        "KPI-007": kpi_discount_rate,
        "KPI-008": kpi_revenue_per_employee,
        "KPI-009": kpi_customer_count,
        "KPI-010": kpi_top10_revenue_share,
    }

    def calculate_all(self):
        """Run all KPIs and populate self.results."""
        for kpi_def in self.kpi_defs:
            kpi_id = kpi_def["id"]
            fn = self.KPI_MAP.get(kpi_id)
            if fn is None:
                continue
            value  = fn(self)
            status = rag_status(value, kpi_def.get("thresholds", {}), kpi_def.get("trend_direction", ""))
            self.results.append({
                "kpi_id":     kpi_id,
                "kpi_name":   kpi_def["name"],
                "category":   kpi_def["category"],
                "value":      round(value, 4),
                "unit":       kpi_def["unit"],
                "rag_status": status,
                "formula":    kpi_def["formula"],
                "calculated_at": datetime.now().isoformat(),
            })
        return self.results

    # ── Monthly Trend ─────────────────────────
    def monthly_trend(self) -> pd.DataFrame:
        """Revenue, GM%, and order count by month."""
        grp = self.eligible.groupby(["year", "month_number", "month_name", "quarter"])
        trend = grp.agg(
            revenue=("sales_amount", "sum"),
            gross_margin=("gross_margin", "sum"),
            cogs=("cogs", "sum"),
            orders=("order_id", "nunique"),
            customers=("customer_key", "nunique"),
            avg_discount=("discount_pct", "mean"),
        ).reset_index()
        trend["gross_margin_pct"] = (trend["gross_margin"] / trend["revenue"] * 100).round(2)
        trend["avg_discount_pct"] = (trend["avg_discount"] * 100).round(2)
        trend["revenue_growth_mom"] = trend["revenue"].pct_change() * 100
        trend = trend.sort_values(["year", "month_number"])
        return trend

    # ── Top N Products ────────────────────────
    def top_products(self, n: int = 10) -> pd.DataFrame:
        dim_product = load("dim_product")
        grp = self.eligible.groupby("product_key").agg(
            revenue=("sales_amount", "sum"),
            gross_margin=("gross_margin", "sum"),
            orders=("order_id", "nunique"),
            quantity=("quantity", "sum"),
        ).reset_index()
        grp = grp.merge(dim_product[["product_key", "product_name", "category"]], on="product_key", how="left")
        grp["gross_margin_pct"] = (grp["gross_margin"] / grp["revenue"] * 100).round(2)
        grp = grp.sort_values("revenue", ascending=False).head(n)
        return grp

    # ── Top N Customers ───────────────────────
    def top_customers(self, n: int = 10) -> pd.DataFrame:
        dim_customer = load("dim_customer")
        grp = self.eligible.groupby("customer_key").agg(
            revenue=("sales_amount", "sum"),
            orders=("order_id", "nunique"),
        ).reset_index()
        grp = grp.merge(dim_customer[["customer_key", "customer_name", "segment"]], on="customer_key", how="left")
        grp["avg_order_value"] = (grp["revenue"] / grp["orders"]).round(2)
        grp = grp.sort_values("revenue", ascending=False).head(n)
        return grp

    # ── Regional Performance ──────────────────
    def regional_performance(self) -> pd.DataFrame:
        dim_region = load("dim_region")
        grp = self.eligible.groupby("region_key").agg(
            revenue=("sales_amount", "sum"),
            gross_margin=("gross_margin", "sum"),
            orders=("order_id", "nunique"),
            customers=("customer_key", "nunique"),
            target=("target_amount", "sum"),
        ).reset_index()
        grp = grp.merge(dim_region[["region_key", "region", "country"]], on="region_key", how="left")
        total_rev = grp["revenue"].sum()
        grp["revenue_share_pct"]   = (grp["revenue"] / total_rev * 100).round(2)
        grp["gross_margin_pct"]    = (grp["gross_margin"] / grp["revenue"] * 100).round(2)
        grp["target_attainment_pct"] = (grp["revenue"] / grp["target"] * 100).round(2)
        grp = grp.sort_values("revenue", ascending=False)
        return grp


# ─────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────
def main():
    print(f"\n{BOLD}{'=' * 65}{RESET}")
    print(f"{BOLD}  SAC ANALYTICS — KPI DASHBOARD REPORT{RESET}")
    print(f"{BOLD}  Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}{RESET}")
    print(f"{BOLD}{'=' * 65}{RESET}\n")

    calc = KPICalculator()
    results = calc.calculate_all()

    # Print KPI table
    print(f"{'KPI':<35} {'Value':>18}  {'Status':>8}")
    print("-" * 65)
    for r in results:
        fval   = format_value(r["value"], r["unit"])
        status = r["rag_status"]
        colour = colour_rag(status)
        icon   = {"GREEN": "🟢", "AMBER": "🟡", "RED": "🔴", "INFO": "🔵"}.get(status, "")
        print(f"{r['kpi_name']:<35} {fval:>18}  {colour}{icon} {status}{RESET}")

    # Monthly trend
    trend = calc.monthly_trend()
    print(f"\n{BOLD}📈 MONTHLY REVENUE TREND (Last 12 Months){RESET}")
    print("-" * 65)
    recent = trend.tail(12)
    for _, row in recent.iterrows():
        bar_len = int(row["revenue"] / trend["revenue"].max() * 30)
        bar = "█" * bar_len
        growth = f"{row['revenue_growth_mom']:+.1f}%" if pd.notna(row["revenue_growth_mom"]) else "  N/A"
        print(f"  {row['year']:.0f}-{row['month_number']:02.0f}  {bar:<32} ${row['revenue']:>12,.0f}  {growth}")

    # Top products
    top_prods = calc.top_products(5)
    print(f"\n{BOLD}🏆 TOP 5 PRODUCTS BY REVENUE{RESET}")
    print("-" * 65)
    for i, (_, row) in enumerate(top_prods.iterrows(), 1):
        print(f"  {i}. {row['product_name']:<40} ${row['revenue']:>12,.0f}  GM: {row['gross_margin_pct']:.1f}%")

    # Regional
    regional = calc.regional_performance()
    print(f"\n{BOLD}🌍 TOP 5 REGIONS BY REVENUE{RESET}")
    print("-" * 65)
    for _, row in regional.head(5).iterrows():
        print(f"  {row['region']:<30} ${row['revenue']:>12,.0f}  Share: {row['revenue_share_pct']:.1f}%  TA: {row['target_attainment_pct']:.0f}%")

    # Save outputs
    results_df = pd.DataFrame(results)
    results_df.to_csv(os.path.join(PROCESSED_DIR, "kpi_results.csv"), index=False)
    trend.to_csv(os.path.join(PROCESSED_DIR, "kpi_monthly_trend.csv"), index=False)
    calc.top_products(20).to_csv(os.path.join(PROCESSED_DIR, "top_products.csv"), index=False)
    calc.top_customers(20).to_csv(os.path.join(PROCESSED_DIR, "top_customers.csv"), index=False)
    regional.to_csv(os.path.join(PROCESSED_DIR, "regional_performance.csv"), index=False)

    print(f"\n{GREEN}✅ KPI results saved to data/processed/{RESET}\n")
    print(f"{'=' * 65}\n")


if __name__ == "__main__":
    main()
