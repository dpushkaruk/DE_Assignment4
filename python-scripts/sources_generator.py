"""
Fire Point — Generate Additional Data Sources (synced with MySQL)
Reads from MySQL to ensure consistency, then generates:
1. JSON: contract negotiation logs (synced with real contractors + products)
2. dbt seeds: product_catalog, currency_rates, component_categories
3. MinIO CSV: employee attendance (synced with real hire_dates + is_active)

Requirements:
    pip install faker mysql-connector-python

Usage:
    1. Run 02_insert_data.py first (MySQL must be populated)
    2. Update DB_CONFIG below
    3. python 03_generate_sources.py
"""

import mysql.connector
import json
import csv
import random
from datetime import timedelta, datetime, date
from pathlib import Path
from faker import Faker

fake = Faker()
Faker.seed(42)
random.seed(42)

BASE = Path(__file__).resolve().parent
JSON_DIR = BASE / "json_sources"
SEED_DIR = BASE / "seeds"
MINIO_DIR = BASE / "minio_data"

for d in [JSON_DIR, SEED_DIR, MINIO_DIR]:
    d.mkdir(parents=True, exist_ok=True)

# ── mysql connection ─────────────────────────────────────────────────────
DB_CONFIG = {
    "host": "localhost",
    "user": "root",
    "password": "mainAWP666",
    "database": "firepoint_db",
}


conn = mysql.connector.connect(**DB_CONFIG)
cur = conn.cursor()
print("connected to mysql\n")



cur.execute("select employee_id, hire_date, is_active from employees")
EMPLOYEES = [(eid, hd, active) for eid, hd, active in cur.fetchall()]
print(f"loaded {len(EMPLOYEES)} employees from mysql")

cur.execute("select contractor_id, name, country from contractors")
CONTRACTORS_DB = [(cid, name, country) for cid, name, country in cur.fetchall()]
print(f"loaded {len(CONTRACTORS_DB)} contractors from mysql")


cur.execute("select product_code, name, base_unit_price from products")
PRODUCTS_DB = [(code, name, float(price)) for code, name, price in cur.fetchall()]
PRODUCT_PRICES = {code: price for code, _, price in PRODUCTS_DB}
PRODUCT_CODES = [code for code, _, _ in PRODUCTS_DB]
print(f"loaded {len(PRODUCTS_DB)} products from mysql")

cur.execute("""
    select so.order_id, c.name, p.product_code, so.quantity,
           so.total_price, so.currency, so.order_date, so.status
    from sales_orders so
    join contractors c on so.contractor_id = c.contractor_id
    join products p on so.product_id = p.product_id
""")
SALES_ORDERS_DB = cur.fetchall()
print(f"loaded {len(SALES_ORDERS_DB)} sales orders from mysql")

cur.close()
conn.close()
print()




negotiations = []


for order_id, contractor_name, product_code, qty, total_price, currency, order_date, status in SALES_ORDERS_DB:
    if status in ("draft", "terminated"):
        continue
    if random.random() > 0.7:  
        continue

    base_price = PRODUCT_PRICES[product_code]
    final_unit_price = round(float(total_price) / qty, 2) if qty > 0 else base_price

    neg_start = order_date - timedelta(days=random.randint(30, 120))
    num_rounds = random.randint(2, 5)

    rounds = []
    current_price = base_price
    round_date = neg_start

    for r in range(1, num_rounds + 1):
        round_date = round_date + timedelta(days=random.randint(3, 21))

        if r % 2 == 1:
            proposed_by = "contractor"
            discount = random.uniform(0.05, 0.25)
            proposed_price = round(current_price * (1 - discount), 2)
        else:
            proposed_by = "fire_point"
            adjustment = random.uniform(-0.05, 0.10)
            proposed_price = round(current_price * (1 + adjustment), 2)

        if r == num_rounds:
            proposed_price = final_unit_price
            outcome = "accepted"
        else:
            outcome = "counter_offered"

        proposed_total = round(proposed_price * qty, 2)

        notes = random.choice([
            None, "Requested volume discount", "Delivery timeline concern",
            "Technical specs discussion", "Payment terms negotiation"
        ])

        rounds.append({
            "round_number": r,
            "date": str(round_date),
            "proposed_by": proposed_by,
            "unit_price_proposed": proposed_price,
            "total_proposed": proposed_total,
            "outcome": outcome,
            "notes": notes,
        })

        current_price = proposed_price

    neg_id = len(negotiations) + 1
    negotiations.append({
        "negotiation_id": f"NEG-{neg_id:04d}",
        "linked_order_id": order_id,
        "contractor": contractor_name,
        "product_code": product_code,
        "quantity": qty,
        "initial_list_price": base_price,
        "start_date": str(neg_start),
        "last_update": str(round_date),
        "num_rounds": num_rounds,
        "rounds": rounds,
        "final_outcome": "contract_signed",
        "final_unit_price": final_unit_price,
        "final_total_value": float(total_price),
        "currency": currency,
        "fire_point_contact": fake.name(),
        "contractor_contact": fake.name(),
    })

for _ in range(100):
    contractor = random.choice(CONTRACTORS_DB)
    ctr_name = contractor[1]
    product_code = random.choice(PRODUCT_CODES)
    base_price = PRODUCT_PRICES[product_code]

    qty = random.randint(2, 30)
    if product_code in ("FP-5", "FP-7", "FP-9"):
        qty = random.randint(1, 8)

    neg_start = fake.date_between(start_date="-2y", end_date="-30d")
    num_rounds = random.randint(1, 4)

    rounds = []
    current_price = base_price
    round_date = neg_start

    for r in range(1, num_rounds + 1):
        round_date = round_date + timedelta(days=random.randint(3, 21))

        if r % 2 == 1:
            proposed_by = "contractor"
            discount = random.uniform(0.05, 0.30)
            proposed_price = round(current_price * (1 - discount), 2)
        else:
            proposed_by = "fire_point"
            adjustment = random.uniform(-0.03, 0.12)
            proposed_price = round(current_price * (1 + adjustment), 2)

        if r == num_rounds:
            outcome = random.choice(["rejected", "counter_offered"])
        else:
            outcome = "counter_offered"

        proposed_total = round(proposed_price * qty, 2)

        notes = random.choice([
            None, "Price too high per contractor", "Budget constraints cited",
            "Competitor offer mentioned", "Delivery timeline unacceptable",
            "Spec mismatch identified", "Political approval pending",
            "Export restrictions flagged",
        ])

        rounds.append({
            "round_number": r,
            "date": str(round_date),
            "proposed_by": proposed_by,
            "unit_price_proposed": proposed_price,
            "total_proposed": proposed_total,
            "outcome": outcome,
            "notes": notes,
        })

        current_price = proposed_price

    last_outcome = rounds[-1]["outcome"]
    final = "deal_lost" if last_outcome == "rejected" else "still_negotiating"

    neg_id = len(negotiations) + 1
    negotiations.append({
        "negotiation_id": f"NEG-{neg_id:04d}",
        "linked_order_id": None,
        "contractor": ctr_name,
        "product_code": product_code,
        "quantity": qty,
        "initial_list_price": base_price,
        "start_date": str(neg_start),
        "last_update": str(round_date),
        "num_rounds": num_rounds,
        "rounds": rounds,
        "final_outcome": final,
        "final_unit_price": None,
        "final_total_value": None,
        "currency": random.choice(["USD", "EUR"]),
        "fire_point_contact": fake.name(),
        "contractor_contact": fake.name(),
    })

with open(JSON_DIR / "contract_negotiations.json", "w") as f:
    json.dump(negotiations, f, indent=2)

signed = sum(1 for n in negotiations if n["final_outcome"] == "contract_signed")
lost = sum(1 for n in negotiations if n["final_outcome"] == "deal_lost")
ongoing = sum(1 for n in negotiations if n["final_outcome"] == "still_negotiating")
print(f"json: contract_negotiations.json — {len(negotiations)} total ({signed} signed, {lost} lost, {ongoing} ongoing)")




with open(SEED_DIR / "seed_product_catalog.csv", "w", newline="") as f:
    w = csv.writer(f)
    w.writerow(["product_code", "product_name", "product_class", "product_type", "max_range_km", "max_speed_ms"])
    w.writerow(["FP-1", "FP-1 Deep Strike UAV", "deep_strike_uav", "uav", 1000, 180])
    w.writerow(["FP-2", "FP-2 Front Strike UAV", "front_strike_uav", "uav", 200, 150])
    w.writerow(["FP-5", "FP-5 Flamingo", "cruise_missile", "missile", 3000, 250])
    w.writerow(["FP-7", "FP-7 Tactical Ballistic Missile", "tactical_ballistic_missile", "missile", 200, 1500])
    w.writerow(["FP-9", "FP-9 Extended Range Ballistic Missile", "ballistic_missile", "missile", 855, 2200])
print("seed: seed_product_catalog.csv — 5 rows")


with open(SEED_DIR / "seed_component_categories.csv", "w", newline="") as f:
    w = csv.writer(f)
    w.writerow(["category_name", "description", "category_group"])
    for row in [
        ("Propulsion", "Engines, motors, fuel systems and thrust components", "mechanical"),
        ("Avionics", "Navigation, flight control, sensors and guidance systems", "electronic"),
        ("Airframe", "Structural components, fuselage, wings and aerodynamic surfaces", "structural"),
        ("Warhead", "Payload delivery, detonation and fragmentation systems", "ordnance"),
        ("Electronics", "Wiring, power supply, communication and imaging modules", "electronic"),
        ("Software", "Firmware, navigation algorithms, encryption and telemetry", "digital"),
    ]:
        w.writerow(row)
print("seed: seed_component_categories.csv — 6 rows")




ATTENDANCE_STATUSES = ["present", "remote", "sick_leave", "vacation", "absent", "business_trip"]
ATTENDANCE_WEIGHTS = [85, 5, 3, 3, 1, 3]

PERIOD_START = date(2025, 1, 1)
PERIOD_END = date(2026, 4, 13)


employee_meta = {}
for eid, hire_date, is_active in EMPLOYEES:
    if is_active == 0:
        days_employed = (PERIOD_END - hire_date).days
        if days_employed > 30:
            left_date = hire_date + timedelta(days=random.randint(30, max(31, days_employed - 30)))
        else:
            left_date = hire_date + timedelta(days=max(1, days_employed))
    else:
        left_date = None
    employee_meta[eid] = {"hire_date": hire_date, "left_date": left_date, "is_active": is_active}


attendance_rows = []
current = PERIOD_START

while current <= PERIOD_END:
    if current.weekday() < 5: 
        for eid, hire_date, is_active in EMPLOYEES:
            meta = employee_meta[eid]
            if current < meta["hire_date"]:
                continue
            if meta["left_date"] and current > meta["left_date"]:
                continue

            status = random.choices(ATTENDANCE_STATUSES, weights=ATTENDANCE_WEIGHTS)[0]

            if status in ("present", "remote"):
                if status == "present":
                    check_in = f"{random.randint(7,9):02d}:{random.randint(0,59):02d}"
                    check_out = f"{random.randint(17,20):02d}:{random.randint(0,59):02d}"
                    hours = round(random.uniform(7.5, 10.5), 1)
                else:
                    check_in = f"{random.randint(8,10):02d}:{random.randint(0,59):02d}"
                    check_out = f"{random.randint(17,19):02d}:{random.randint(0,59):02d}"
                    hours = round(random.uniform(7.0, 9.5), 1)
            else:
                check_in = ""
                check_out = ""
                hours = 0.0

            attendance_rows.append([
                eid, str(current), status, check_in, check_out, hours
            ])

    current += timedelta(days=1)

with open(MINIO_DIR / "employee_attendance.csv", "w", newline="") as f:
    w = csv.writer(f)
    w.writerow(["employee_id", "date", "status", "check_in", "check_out", "hours_worked"])
    w.writerows(attendance_rows)

print(f"minio: employee_attendance.csv — {len(attendance_rows):,} rows")


active_emps = sum(1 for e in EMPLOYEES if e[2] == 1)
inactive_emps = sum(1 for e in EMPLOYEES if e[2] == 0)
hired_after_start = sum(1 for e in EMPLOYEES if e[1] > PERIOD_START)
print(f"  active employees: {active_emps}, inactive: {inactive_emps}")
print(f"  hired after {PERIOD_START}: {hired_after_start} (shorter attendance records)")



print("\n" + "=" * 50)
print("  all sources generated (synced with mysql)")
print("=" * 50)
print(f"  json_sources/contract_negotiations.json   {len(negotiations):>8} negotiations")
print(f"  seeds/seed_product_catalog.csv            {5:>8} rows")
print(f"  seeds/seed_component_categories.csv       {6:>8} rows")
print(f"  minio_data/employee_attendance.csv        {len(attendance_rows):>8,} rows")
print("\ndone")