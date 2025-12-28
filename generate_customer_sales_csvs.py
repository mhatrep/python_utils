import csv
import random
import string
import os
from datetime import datetime, timedelta

OUT_DIR = "test_csvs"
os.makedirs(OUT_DIR, exist_ok=True)

ROW_LIMIT = 1000
random.seed(42)

# ------------------------
# Helper functions
# ------------------------
def rand_date(start_year=2022, end_year=2025):
    start = datetime(start_year, 1, 1)
    end = datetime(end_year, 12, 31)
    delta = end - start
    return (start + timedelta(days=random.randint(0, delta.days))).strftime("%Y-%m-%d")

def rand_amount(a=10, b=5000):
    return round(random.uniform(a, b), 2)

def rand_text(prefix):
    return f"{prefix}_{random.randint(1000,9999)}"

# ------------------------
# Reference tables
# ------------------------
countries = [
    ("US", "United States"),
    ("IN", "India"),
    ("DE", "Germany"),
    ("UK", "United Kingdom"),
    ("CA", "Canada"),
]

segments = [
    ("ENT", "Enterprise"),
    ("SMB", "Small Business"),
    ("CON", "Consumer"),
]

categories = ["Electronics", "Office", "Furniture", "Accessories"]

# ------------------------
# customers.csv
# ------------------------
customers = []
for i in range(1, ROW_LIMIT + 1):
    customers.append({
        "customer_id": f"CUST{i:05d}",
        "customer_name": f"Customer {i}",
        "email": f"customer{i}@example.com",
        "country_code": random.choice(countries)[0],
        "segment_code": random.choice(segments)[0],
        "status": random.choice(["ACTIVE", "INACTIVE"]),
        "created_date": rand_date(),
    })

with open(f"{OUT_DIR}/customers.csv", "w", newline="", encoding="utf-8") as f:
    w = csv.DictWriter(f, fieldnames=customers[0].keys())
    w.writeheader()
    w.writerows(customers)

# ------------------------
# sales_reps.csv
# ------------------------
sales_reps = []
for i in range(1, 101):
    sales_reps.append({
        "sales_rep_id": f"REP{i:03d}",
        "sales_rep_name": f"Rep {i}",
        "region": random.choice(["NA", "EU", "APAC"]),
        "status": random.choice(["ACTIVE", "INACTIVE"]),
    })

with open(f"{OUT_DIR}/sales_reps.csv", "w", newline="", encoding="utf-8") as f:
    w = csv.DictWriter(f, fieldnames=sales_reps[0].keys())
    w.writeheader()
    w.writerows(sales_reps)

# ------------------------
# products.csv
# ------------------------
products = []
for i in range(1, 501):
    products.append({
        "product_id": f"PROD{i:04d}",
        "product_name": f"Product {i}",
        "category": random.choice(categories),
        "unit_price": rand_amount(5, 500),
        "status": random.choice(["ACTIVE", "DISCONTINUED"]),
    })

with open(f"{OUT_DIR}/products.csv", "w", newline="", encoding="utf-8") as f:
    w = csv.DictWriter(f, fieldnames=products[0].keys())
    w.writeheader()
    w.writerows(products)

# ------------------------
# orders.csv
# ------------------------
orders = []
for i in range(1, ROW_LIMIT + 1):
    cust = random.choice(customers)
    rep = random.choice(sales_reps)
    orders.append({
        "order_id": f"ORD{i:06d}",
        "customer_id": cust["customer_id"],
        "sales_rep_id": rep["sales_rep_id"],
        "order_date": rand_date(),
        "order_status": random.choice(["NEW", "SHIPPED", "CANCELLED", "COMPLETE"]),
        "order_total": rand_amount(),
    })

with open(f"{OUT_DIR}/orders.csv", "w", newline="", encoding="utf-8") as f:
    w = csv.DictWriter(f, fieldnames=orders[0].keys())
    w.writeheader()
    w.writerows(orders)

# ------------------------
# order_items.csv
# ------------------------
order_items = []
for o in orders:
    for _ in range(random.randint(1, 5)):
        p = random.choice(products)
        order_items.append({
            "order_id": o["order_id"],
            "product_id": p["product_id"],
            "quantity": random.randint(1, 10),
            "line_amount": rand_amount(10, 1000),
        })

with open(f"{OUT_DIR}/order_items.csv", "w", newline="", encoding="utf-8") as f:
    w = csv.DictWriter(f, fieldnames=order_items[0].keys())
    w.writeheader()
    w.writerows(order_items[:ROW_LIMIT])

# ------------------------
# payments.csv
# ------------------------
payments = []
for o in orders:
    payments.append({
        "payment_id": rand_text("PAY"),
        "order_id": o["order_id"],
        "payment_method": random.choice(["CARD", "WIRE", "UPI", "PAYPAL"]),
        "payment_amount": o["order_total"],
        "payment_date": o["order_date"],
    })

with open(f"{OUT_DIR}/payments.csv", "w", newline="", encoding="utf-8") as f:
    w = csv.DictWriter(f, fieldnames=payments[0].keys())
    w.writeheader()
    w.writerows(payments)

# ------------------------
# shipments.csv
# ------------------------
shipments = []
for o in orders:
    shipments.append({
        "shipment_id": rand_text("SHIP"),
        "order_id": o["order_id"],
        "ship_date": rand_date(),
        "carrier": random.choice(["UPS", "FedEx", "DHL"]),
        "delivery_status": random.choice(["IN_TRANSIT", "DELIVERED"]),
    })

with open(f"{OUT_DIR}/shipments.csv", "w", newline="", encoding="utf-8") as f:
    w = csv.DictWriter(f, fieldnames=shipments[0].keys())
    w.writeheader()
    w.writerows(shipments)

# ------------------------
# returns.csv
# ------------------------
returns = []
for o in random.sample(orders, k=200):
    returns.append({
        "return_id": rand_text("RET"),
        "order_id": o["order_id"],
        "return_reason": random.choice(["DAMAGED", "NOT_REQUIRED", "WRONG_ITEM"]),
        "return_date": rand_date(),
        "comments": f"Return for {o['order_id']} by customer",
    })

with open(f"{OUT_DIR}/returns.csv", "w", newline="", encoding="utf-8") as f:
    w = csv.DictWriter(f, fieldnames=returns[0].keys())
    w.writeheader()
    w.writerows(returns)

# ------------------------
# countries.csv
# ------------------------
with open(f"{OUT_DIR}/countries.csv", "w", newline="", encoding="utf-8") as f:
    w = csv.writer(f)
    w.writerow(["country_code", "country_name"])
    w.writerows(countries)

# ------------------------
# segments.csv
# ------------------------
with open(f"{OUT_DIR}/segments.csv", "w", newline="", encoding="utf-8") as f:
    w = csv.writer(f)
    w.writerow(["segment_code", "segment_name"])
    w.writerows(segments)

print(f"Generated 10 CSV files in folder: {OUT_DIR}")
