"""
generate_sample_data.py
-----------------------
Generates realistic synthetic ERP/CRM source data simulating:
  - Sales transactions (24 months)
  - Product catalog
  - Customer master
  - Employee/rep data
  - Regional hierarchy

Output: CSV files in data/raw/
"""

import os
import random
import string
import pandas as pd
import numpy as np
from datetime import date, timedelta, datetime

random.seed(42)
np.random.seed(42)

# ─── Lightweight fake-data helpers (no Faker dependency) ───
_FIRST = ["James","Mary","John","Patricia","Robert","Jennifer","Michael","Linda",
          "William","Barbara","David","Susan","Richard","Jessica","Joseph","Sarah",
          "Thomas","Karen","Charles","Lisa","Daniel","Nancy","Matthew","Betty",
          "Anthony","Margaret","Mark","Sandra","Donald","Ashley"]
_LAST  = ["Smith","Johnson","Williams","Brown","Jones","Garcia","Miller","Davis",
          "Rodriguez","Martinez","Hernandez","Lopez","Gonzalez","Wilson","Anderson",
          "Thomas","Taylor","Moore","Jackson","Martin","Lee","Perez","Thompson","White"]
_CO    = ["Corp","Inc","Ltd","Group","Solutions","Systems","Technologies","Partners",
          "Enterprises","Global","Holdings","Services","Consulting","Dynamics"]
_COBASE= ["Apex","Blue","Cedar","Delta","Echo","Falcon","Global","Harbor","Iris",
          "Juno","Kite","Luxe","Maple","Nexus","Orbit","Prism","Quest","Rapid",
          "Solar","Titan","Unity","Vertex","Wave","Xeon","Yield","Zenith"]

def _name():
    return f"{random.choice(_FIRST)} {random.choice(_LAST)}"

def _company():
    return f"{random.choice(_COBASE)} {random.choice(_CO)}"

def _email(company_name: str):
    slug = company_name.lower().replace(" ", "").replace(",","")[:12]
    return f"contact@{slug}.com"

def _date_between(start: date, end: date) -> date:
    delta = (end - start).days
    return start + timedelta(days=random.randint(0, delta))

def _bothify(pattern: str) -> str:
    result = ""
    for ch in pattern:
        if ch == '?':
            result += random.choice(string.ascii_uppercase)
        elif ch == '#':
            result += str(random.randint(0, 9))
        else:
            result += ch
    return result

OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "..", "data", "raw")
os.makedirs(OUTPUT_DIR, exist_ok=True)

# ─────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────
START_DATE   = date(2024, 1, 1)
END_DATE     = date(2025, 12, 31)
N_PRODUCTS   = 80
N_CUSTOMERS  = 300
N_EMPLOYEES  = 40
N_REGIONS    = 20
N_ORDERS     = 8000


# ─────────────────────────────────────────────
# DIMENSION: REGIONS
# ─────────────────────────────────────────────
def generate_regions(n: int) -> pd.DataFrame:
    regions_data = [
        ("United States", "North America", "Northeast US",  "New York",    "USD"),
        ("United States", "North America", "Southeast US",  "Atlanta",     "USD"),
        ("United States", "North America", "Midwest US",    "Chicago",     "USD"),
        ("United States", "North America", "West US",       "Los Angeles", "USD"),
        ("United States", "North America", "Southwest US",  "Dallas",      "USD"),
        ("Canada",        "North America", "Eastern Canada","Toronto",     "CAD"),
        ("Canada",        "North America", "Western Canada","Vancouver",   "CAD"),
        ("United Kingdom","Europe",        "England",       "London",      "GBP"),
        ("Germany",       "Europe",        "West Germany",  "Frankfurt",   "EUR"),
        ("France",        "Europe",        "Ile-de-France", "Paris",       "EUR"),
        ("Netherlands",   "Europe",        "North Holland", "Amsterdam",   "EUR"),
        ("Spain",         "Europe",        "Catalonia",     "Barcelona",   "EUR"),
        ("Australia",     "Asia Pacific",  "New South Wales","Sydney",     "AUD"),
        ("Australia",     "Asia Pacific",  "Victoria",      "Melbourne",   "AUD"),
        ("Japan",         "Asia Pacific",  "Kanto",         "Tokyo",       "JPY"),
        ("Singapore",     "Asia Pacific",  "Central Region","Singapore",   "SGD"),
        ("India",         "Asia Pacific",  "Maharashtra",   "Mumbai",      "INR"),
        ("Brazil",        "Latin America", "Southeast",     "São Paulo",   "BRL"),
        ("Mexico",        "Latin America", "Mexico City",   "Mexico City", "MXN"),
        ("UAE",           "Middle East",   "Dubai",         "Dubai",       "AED"),
    ]
    rows = []
    for i, (country, region, sub_region, city, currency) in enumerate(regions_data[:n], start=1):
        rows.append({
            "region_key": i,
            "country":    country,
            "region":     region,
            "sub_region": sub_region,
            "city":       city,
            "currency":   currency,
        })
    return pd.DataFrame(rows)


# ─────────────────────────────────────────────
# DIMENSION: PRODUCTS
# ─────────────────────────────────────────────
def generate_products(n: int) -> pd.DataFrame:
    categories = {
        "Electronics":    ["Laptops", "Tablets", "Monitors", "Peripherals"],
        "Software":       ["ERP Licenses", "Analytics Tools", "Security Suite", "Cloud Services"],
        "Services":       ["Consulting", "Implementation", "Support & Maintenance", "Training"],
        "Hardware":       ["Servers", "Networking", "Storage", "IoT Devices"],
        "Office Supplies":["Furniture", "Stationery", "Equipment", "Accessories"],
    }
    brands = ["TechCorp", "SoftPro", "GlobalSystems", "InnovateTech", "DataStream", "CloudEdge"]
    rows = []
    for i in range(1, n + 1):
        cat = random.choice(list(categories.keys()))
        sub = random.choice(categories[cat])
        unit_cost  = round(random.uniform(50, 5000), 2)
        list_price = round(unit_cost * random.uniform(1.3, 2.5), 2)
        rows.append({
            "product_key":  i,
            "product_id":   f"PRD-{i:04d}",
            "product_name": f"{random.choice(brands)} {sub} {_bothify('??-###')}",
            "category":     cat,
            "sub_category": sub,
            "brand":        random.choice(brands),
            "unit_cost":    unit_cost,
            "list_price":   list_price,
            "is_active":    random.choices([True, False], weights=[90, 10])[0],
            "launch_date":  _date_between(date(2020, 1, 1), START_DATE),
        })
    return pd.DataFrame(rows)


# ─────────────────────────────────────────────
# DIMENSION: CUSTOMERS
# ─────────────────────────────────────────────
def generate_customers(n: int) -> pd.DataFrame:
    segments  = ["Enterprise", "Mid-Market", "SMB", "Startup"]
    industries = [
        "Manufacturing", "Financial Services", "Healthcare", "Retail",
        "Technology", "Energy", "Telecommunications", "Education",
        "Government", "Logistics",
    ]
    rows = []
    for i in range(1, n + 1):
        cname = _company()
        rows.append({
            "customer_key":     i,
            "customer_id":      f"CUST-{i:05d}",
            "customer_name":    cname,
            "segment":          random.choice(segments),
            "industry":         random.choice(industries),
            "email":            _email(cname),
            "acquisition_date": _date_between(date(2019, 1, 1), START_DATE),
            "is_active":        random.choices([True, False], weights=[85, 15])[0],
        })
    return pd.DataFrame(rows)


# ─────────────────────────────────────────────
# DIMENSION: EMPLOYEES
# ─────────────────────────────────────────────
def generate_employees(n: int, region_keys: list) -> pd.DataFrame:
    titles = [
        "Account Executive", "Senior Account Executive",
        "Sales Manager", "Regional Sales Director",
        "Business Development Rep", "Inside Sales Rep",
    ]
    departments = ["Sales", "Pre-Sales", "Channel Sales", "Enterprise Sales"]
    rows = []
    for i in range(1, n + 1):
        rows.append({
            "employee_key": i,
            "employee_id":  f"EMP-{i:03d}",
            "full_name":    _name(),
            "department":   random.choice(departments),
            "job_title":    random.choice(titles),
            "manager_id":   f"EMP-{random.randint(1, max(1, n // 5)):03d}" if i > 5 else None,
            "hire_date":    _date_between(date(2018, 1, 1), START_DATE),
            "region_key":   random.choice(region_keys),
            "is_active":    random.choices([True, False], weights=[92, 8])[0],
        })
    return pd.DataFrame(rows)


# ─────────────────────────────────────────────
# DIMENSION: DATE
# ─────────────────────────────────────────────
def generate_date_dimension(start: date, end: date) -> pd.DataFrame:
    rows = []
    current = start
    holidays = {
        date(2024, 1, 1), date(2024, 7, 4), date(2024, 12, 25),
        date(2025, 1, 1), date(2025, 7, 4), date(2025, 12, 25),
    }
    while current <= end:
        q = f"Q{(current.month - 1) // 3 + 1}"
        rows.append({
            "date_key":      int(current.strftime("%Y%m%d")),
            "full_date":     current,
            "day_of_week":   current.strftime("%A"),
            "day_of_month":  current.day,
            "week_number":   current.isocalendar()[1],
            "month_number":  current.month,
            "month_name":    current.strftime("%B"),
            "quarter":       q,
            "year":          current.year,
            "is_weekend":    current.weekday() >= 5,
            "is_holiday":    current in holidays,
            "fiscal_period": f"FY{current.year}-P{current.month:02d}",
        })
        current += timedelta(days=1)
    return pd.DataFrame(rows)


# ─────────────────────────────────────────────
# FACT: SALES
# ─────────────────────────────────────────────
def generate_sales(
    n_orders: int,
    products: pd.DataFrame,
    customers: pd.DataFrame,
    employees: pd.DataFrame,
    regions: pd.DataFrame,
    dates: pd.DataFrame,
) -> pd.DataFrame:
    date_keys    = dates["date_key"].tolist()
    product_keys = products["product_key"].tolist()
    customer_keys= customers["customer_key"].tolist()
    employee_keys= employees["employee_key"].tolist()
    region_keys  = regions["region_key"].tolist()

    statuses = ["Open", "Confirmed", "Shipped", "Delivered", "Cancelled"]
    status_weights = [5, 10, 20, 60, 5]
    channels = ["Direct", "Partner", "Online", "Retail"]
    channel_weights = [40, 30, 20, 10]

    prod_lookup = products.set_index("product_key")[["unit_cost", "list_price"]].to_dict("index")

    rows = []
    sales_key = 1
    for order_num in range(1, n_orders + 1):
        order_id   = f"ORD-{order_num:06d}"
        order_date = random.choice(date_keys)
        customer   = random.choice(customer_keys)
        employee   = random.choice(employee_keys)
        region     = random.choice(region_keys)
        channel    = random.choices(channels, weights=channel_weights)[0]
        status     = random.choices(statuses, weights=status_weights)[0]
        n_lines    = random.randint(1, 5)
        ts = datetime.now().replace(
            hour=random.randint(8,18),
            minute=random.randint(0,59),
            second=random.randint(0,59),
            microsecond=0
        ) - timedelta(days=random.randint(0, 730))

        for line in range(1, n_lines + 1):
            product    = random.choice(product_keys)
            pdata      = prod_lookup[product]
            quantity   = random.randint(1, 50)
            discount   = round(random.choices(
                [0, 0.05, 0.10, 0.15, 0.20, 0.25],
                weights=[30, 20, 20, 15, 10, 5]
            )[0], 2)
            unit_price = round(pdata["list_price"] * (1 - discount / 2), 2)
            sales_amt  = round(quantity * unit_price * (1 - discount), 2)
            cogs       = round(quantity * pdata["unit_cost"], 2)
            gm         = round(sales_amt - cogs, 2)
            target     = round(sales_amt * random.uniform(0.85, 1.20), 2)

            rows.append({
                "sales_key":      sales_key,
                "order_id":       order_id,
                "line_number":    line,
                "date_key":       order_date,
                "product_key":    product,
                "customer_key":   customer,
                "region_key":     region,
                "employee_key":   employee,
                "quantity":       quantity,
                "unit_price":     unit_price,
                "discount_pct":   discount,
                "sales_amount":   sales_amt,
                "cogs":           cogs,
                "gross_margin":   gm,
                "target_amount":  target,
                "order_status":   status,
                "channel":        channel,
                "created_at":     ts,
                "updated_at":     ts,
            })
            sales_key += 1

    return pd.DataFrame(rows)


# ─────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────
def main():
    print("🔄 Generating SAC Analytics sample data...\n")

    print("  ✅ Generating date dimension...")
    dates = generate_date_dimension(START_DATE, END_DATE)
    dates.to_csv(f"{OUTPUT_DIR}/dim_date.csv", index=False)
    print(f"     → {len(dates):,} date records")

    print("  ✅ Generating region dimension...")
    regions = generate_regions(N_REGIONS)
    regions.to_csv(f"{OUTPUT_DIR}/dim_region.csv", index=False)
    print(f"     → {len(regions):,} regions")

    print("  ✅ Generating product dimension...")
    products = generate_products(N_PRODUCTS)
    products.to_csv(f"{OUTPUT_DIR}/dim_product.csv", index=False)
    print(f"     → {len(products):,} products")

    print("  ✅ Generating customer dimension...")
    customers = generate_customers(N_CUSTOMERS)
    customers.to_csv(f"{OUTPUT_DIR}/dim_customer.csv", index=False)
    print(f"     → {len(customers):,} customers")

    print("  ✅ Generating employee dimension...")
    employees = generate_employees(N_EMPLOYEES, regions["region_key"].tolist())
    employees.to_csv(f"{OUTPUT_DIR}/dim_employee.csv", index=False)
    print(f"     → {len(employees):,} employees")

    print("  ✅ Generating sales fact table...")
    sales = generate_sales(N_ORDERS, products, customers, employees, regions, dates)
    sales.to_csv(f"{OUTPUT_DIR}/fact_sales.csv", index=False)
    print(f"     → {len(sales):,} sales line items")

    total_rev = sales["sales_amount"].sum()
    print(f"\n📊 Summary:")
    print(f"   Total Revenue:    ${total_rev:>15,.2f}")
    print(f"   Total Orders:     {sales['order_id'].nunique():>10,}")
    print(f"   Date Range:       {START_DATE} → {END_DATE}")
    print(f"\n✅ All files saved to: {OUTPUT_DIR}/\n")


if __name__ == "__main__":
    main()
