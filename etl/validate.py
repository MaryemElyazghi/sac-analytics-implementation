"""
validate.py
-----------
Data quality validation engine for the SAC star schema.

Checks:
  - Null / blank values in key columns
  - Duplicate primary keys
  - Referential integrity (FK checks)
  - Business rule validations (no negative revenue, price > cost, etc.)
  - Threshold checks for KPI sanity

Run: python etl/validate.py
"""

import os
import sys
import pandas as pd
from dataclasses import dataclass, field
from typing import List

PROCESSED_DIR = os.path.join(os.path.dirname(__file__), "..", "data", "processed")


# ─────────────────────────────────────────────
# RESULT CLASSES
# ─────────────────────────────────────────────
@dataclass
class ValidationResult:
    check_name:  str
    table:       str
    passed:      bool
    message:     str
    severity:    str = "ERROR"   # ERROR | WARNING | INFO
    row_count:   int = 0


@dataclass
class ValidationReport:
    results: List[ValidationResult] = field(default_factory=list)

    def add(self, result: ValidationResult):
        self.results.append(result)

    @property
    def errors(self):
        return [r for r in self.results if not r.passed and r.severity == "ERROR"]

    @property
    def warnings(self):
        return [r for r in self.results if not r.passed and r.severity == "WARNING"]

    @property
    def passed_count(self):
        return sum(1 for r in self.results if r.passed)

    def print_summary(self):
        total   = len(self.results)
        passed  = self.passed_count
        failed  = total - passed
        err_cnt = len(self.errors)
        wrn_cnt = len(self.warnings)

        print("\n" + "=" * 65)
        print("  DATA QUALITY VALIDATION REPORT")
        print("=" * 65)
        print(f"  Total Checks : {total}")
        print(f"  ✅ Passed    : {passed}")
        print(f"  ❌ Errors    : {err_cnt}")
        print(f"  ⚠️  Warnings  : {wrn_cnt}")
        print("=" * 65)

        if self.errors:
            print("\n❌ ERRORS:")
            for r in self.errors:
                print(f"   [{r.table}] {r.check_name}: {r.message} (rows: {r.row_count})")

        if self.warnings:
            print("\n⚠️  WARNINGS:")
            for r in self.warnings:
                print(f"   [{r.table}] {r.check_name}: {r.message} (rows: {r.row_count})")

        passing = [r for r in self.results if r.passed]
        if passing:
            print("\n✅ PASSED CHECKS:")
            for r in passing:
                print(f"   [{r.table}] {r.check_name}")

        print("=" * 65)
        return err_cnt == 0


# ─────────────────────────────────────────────
# GENERIC CHECKS
# ─────────────────────────────────────────────
def check_no_nulls(df: pd.DataFrame, table: str, columns: List[str], report: ValidationReport):
    for col in columns:
        null_count = df[col].isnull().sum()
        passed = null_count == 0
        report.add(ValidationResult(
            check_name=f"no_nulls.{col}",
            table=table,
            passed=passed,
            message=f"{null_count} null values in column '{col}'" if not passed else "OK",
            severity="ERROR",
            row_count=null_count,
        ))


def check_no_duplicates(df: pd.DataFrame, table: str, pk: str, report: ValidationReport):
    dup_count = df[pk].duplicated().sum()
    passed = dup_count == 0
    report.add(ValidationResult(
        check_name=f"no_duplicate_pk.{pk}",
        table=table,
        passed=passed,
        message=f"{dup_count} duplicate values in PK '{pk}'" if not passed else "OK",
        severity="ERROR",
        row_count=dup_count,
    ))


def check_fk_integrity(
    df: pd.DataFrame, fk_col: str,
    ref_df: pd.DataFrame, ref_col: str,
    table: str, ref_table: str,
    report: ValidationReport,
):
    orphan_count = (~df[fk_col].isin(ref_df[ref_col])).sum()
    passed = orphan_count == 0
    report.add(ValidationResult(
        check_name=f"fk.{fk_col} → {ref_table}.{ref_col}",
        table=table,
        passed=passed,
        message=f"{orphan_count} orphan FK values (no match in {ref_table}.{ref_col})" if not passed else "OK",
        severity="ERROR",
        row_count=orphan_count,
    ))


def check_value_range(
    df: pd.DataFrame, table: str, col: str,
    min_val=None, max_val=None,
    report: ValidationReport = None,
    severity: str = "ERROR",
):
    mask = pd.Series([False] * len(df), index=df.index)
    if min_val is not None:
        mask = mask | (df[col] < min_val)
    if max_val is not None:
        mask = mask | (df[col] > max_val)
    out_of_range = mask.sum()
    passed = out_of_range == 0
    label = f"[{min_val}, {max_val}]"
    report.add(ValidationResult(
        check_name=f"range.{col} in {label}",
        table=table,
        passed=passed,
        message=f"{out_of_range} values outside range {label}" if not passed else "OK",
        severity=severity,
        row_count=out_of_range,
    ))


# ─────────────────────────────────────────────
# TABLE-SPECIFIC VALIDATIONS
# ─────────────────────────────────────────────
def validate_dim_date(df: pd.DataFrame, report: ValidationReport):
    check_no_nulls(df, "dim_date", ["date_key", "full_date", "year", "month_number"], report)
    check_no_duplicates(df, "dim_date", "date_key", report)
    check_value_range(df, "dim_date", "month_number", 1, 12, report)
    check_value_range(df, "dim_date", "day_of_month", 1, 31, report)
    check_value_range(df, "dim_date", "year", 2000, 2030, report)

    q_values = df["quarter"].unique()
    invalid_q = [q for q in q_values if q not in ("Q1", "Q2", "Q3", "Q4")]
    report.add(ValidationResult(
        check_name="valid_quarter_values",
        table="dim_date",
        passed=len(invalid_q) == 0,
        message=f"Invalid quarter values: {invalid_q}" if invalid_q else "OK",
        severity="ERROR",
        row_count=len(invalid_q),
    ))


def validate_dim_product(df: pd.DataFrame, report: ValidationReport):
    check_no_nulls(df, "dim_product", ["product_key", "product_id", "product_name", "category", "unit_cost", "list_price"], report)
    check_no_duplicates(df, "dim_product", "product_key", report)
    check_value_range(df, "dim_product", "unit_cost", 0, None, report)
    check_value_range(df, "dim_product", "list_price", 0, None, report)

    # list_price must be >= unit_cost
    inverted = (df["list_price"] < df["unit_cost"]).sum()
    report.add(ValidationResult(
        check_name="list_price >= unit_cost",
        table="dim_product",
        passed=inverted == 0,
        message=f"{inverted} products where list_price < unit_cost" if inverted else "OK",
        severity="WARNING",
        row_count=inverted,
    ))


def validate_dim_customer(df: pd.DataFrame, report: ValidationReport):
    check_no_nulls(df, "dim_customer", ["customer_key", "customer_id", "customer_name", "segment"], report)
    check_no_duplicates(df, "dim_customer", "customer_key", report)

    valid_segments = {"Enterprise", "Mid-Market", "SMB", "Startup"}
    invalid_segs = (~df["segment"].isin(valid_segments)).sum()
    report.add(ValidationResult(
        check_name="valid_segment_values",
        table="dim_customer",
        passed=invalid_segs == 0,
        message=f"{invalid_segs} rows with invalid segment values" if invalid_segs else "OK",
        severity="WARNING",
        row_count=invalid_segs,
    ))


def validate_dim_employee(df: pd.DataFrame, report: ValidationReport):
    check_no_nulls(df, "dim_employee", ["employee_key", "employee_id", "full_name", "department"], report)
    check_no_duplicates(df, "dim_employee", "employee_key", report)


def validate_dim_region(df: pd.DataFrame, report: ValidationReport):
    check_no_nulls(df, "dim_region", ["region_key", "country", "region", "city", "currency"], report)
    check_no_duplicates(df, "dim_region", "region_key", report)


def validate_fact_sales(
    df: pd.DataFrame,
    dim_date: pd.DataFrame, dim_product: pd.DataFrame,
    dim_customer: pd.DataFrame, dim_employee: pd.DataFrame,
    dim_region: pd.DataFrame,
    report: ValidationReport,
):
    check_no_nulls(df, "fact_sales", [
        "sales_key", "order_id", "date_key", "product_key",
        "customer_key", "region_key", "employee_key",
        "quantity", "unit_price", "sales_amount", "cogs"
    ], report)
    check_no_duplicates(df, "fact_sales", "sales_key", report)

    # FK checks
    check_fk_integrity(df, "date_key",     dim_date,     "date_key",     "fact_sales", "dim_date",     report)
    check_fk_integrity(df, "product_key",  dim_product,  "product_key",  "fact_sales", "dim_product",  report)
    check_fk_integrity(df, "customer_key", dim_customer, "customer_key", "fact_sales", "dim_customer", report)
    check_fk_integrity(df, "employee_key", dim_employee, "employee_key", "fact_sales", "dim_employee", report)
    check_fk_integrity(df, "region_key",   dim_region,   "region_key",   "fact_sales", "dim_region",   report)

    # Business rule checks
    check_value_range(df, "fact_sales", "quantity",     1,    None, report, "ERROR")
    check_value_range(df, "fact_sales", "unit_price",   0.01, None, report, "ERROR")
    check_value_range(df, "fact_sales", "discount_pct", 0,    1.0,  report, "ERROR")
    check_value_range(df, "fact_sales", "sales_amount", 0,    None, report, "ERROR")
    check_value_range(df, "fact_sales", "cogs",         0,    None, report, "ERROR")

    # Gross margin sanity: GM should be < sales_amount
    gm_anomaly = (df["gross_margin"] > df["sales_amount"]).sum()
    report.add(ValidationResult(
        check_name="gross_margin <= sales_amount",
        table="fact_sales",
        passed=gm_anomaly == 0,
        message=f"{gm_anomaly} rows where gross_margin > sales_amount" if gm_anomaly else "OK",
        severity="WARNING",
        row_count=gm_anomaly,
    ))

    # Valid order statuses
    valid_statuses = {"Open", "Confirmed", "Shipped", "Delivered", "Cancelled"}
    invalid_status = (~df["order_status"].isin(valid_statuses)).sum()
    report.add(ValidationResult(
        check_name="valid_order_status",
        table="fact_sales",
        passed=invalid_status == 0,
        message=f"{invalid_status} rows with invalid order_status" if invalid_status else "OK",
        severity="ERROR",
        row_count=invalid_status,
    ))


# ─────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────
def load(table: str) -> pd.DataFrame:
    return pd.read_csv(os.path.join(PROCESSED_DIR, f"{table}.csv"))


def run_validation() -> bool:
    print("\n🔍 Running SAC Analytics Data Quality Validation...\n")
    report = ValidationReport()

    dim_date     = load("dim_date")
    dim_product  = load("dim_product")
    dim_customer = load("dim_customer")
    dim_employee = load("dim_employee")
    dim_region   = load("dim_region")
    fact_sales   = load("fact_sales")

    validate_dim_date(dim_date, report)
    validate_dim_product(dim_product, report)
    validate_dim_customer(dim_customer, report)
    validate_dim_employee(dim_employee, report)
    validate_dim_region(dim_region, report)
    validate_fact_sales(
        fact_sales, dim_date, dim_product,
        dim_customer, dim_employee, dim_region, report
    )

    all_passed = report.print_summary()
    if not all_passed:
        print("\n❌ Validation FAILED — fix errors before loading to SAC.\n")
        sys.exit(1)
    else:
        print("\n✅ All validations PASSED — data is ready for SAC ingestion.\n")
    return all_passed


if __name__ == "__main__":
    run_validation()
