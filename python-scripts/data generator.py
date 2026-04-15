import mysql.connector
import random
from datetime import timedelta
from faker import Faker
 
fake = Faker()
Faker.seed(42)
random.seed(42)
 

DB_CONFIG = {
    "host": "localhost",
    "user": "root",
    "password": "secret",
    "database": "firepoint_db",
}
 
conn = mysql.connector.connect(**DB_CONFIG)
cur = conn.cursor()
print("connected to mysql\n")

cur.execute("SET FOREIGN_KEY_CHECKS = 0")
for t in [
    "milestones", "research_projects", "payments", "invoices", "budgets", "production_stages", "production_orders",
    "sales_orders", "products", "production_lines", "purchase_orders",
    "components", "suppliers", "contractors", "employees", "departments",
]:
    cur.execute(f"TRUNCATE TABLE {t}")
cur.execute("SET FOREIGN_KEY_CHECKS = 1")
conn.commit()
print("tables truncated\n")

#deps
DEPT_NAMES = ["Production", "Procurement", "R&D", "Sales", "Finance", "HR"]
 
for name in DEPT_NAMES:
    cur.execute("insert into departments (name) values (%s)", (name,))
 
conn.commit()
 
cur.execute("select department_id, name from departments")
DEPT_MAP = {name: id for id, name in cur.fetchall()}
print(f"departments: {len(DEPT_MAP)}")

#employees
ROLES_BY_DEPT = {
    "Production":  ["Production Manager", "Assembly Technician", "Quality Inspector",
                     "Production Engineer", "Shift Supervisor"],
    "Procurement": ["Procurement Manager", "Procurement Specialist", "Supply Chain Analyst",
                     "Logistics Coordinator", "Vendor Manager"],
    "R&D":         ["R&D Director", "Aeronautics Engineer", "Software Engineer",
                     "Systems Architect", "Test Engineer"],
    "Sales":       ["Sales Director", "Account Manager", "Contract Specialist",
                     "Business Development Manager"],
    "Finance":     ["CFO", "Financial Analyst", "Accountant",
                     "Budget Controller"],
    "HR":          ["HR Director", "HR Specialist", "Recruiter",
                     "Training Coordinator"],
}

DEPT_SIZES = {
    "Production": 100,
    "Procurement": 20,
    "R&D": 50,
    "Sales": 10,
    "Finance": 5,
    "HR": 5}

emp_counter = 0
 
for dept_name, count in DEPT_SIZES.items():
    dept_id = DEPT_MAP[dept_name]
    roles = ROLES_BY_DEPT[dept_name]
 
    for i in range(count):
        emp_counter += 1
        fn = fake.first_name()
        ln = fake.last_name()
        email = f"{fn.lower()}.{ln.lower()}@firepoint.agency"
        hire_date = fake.date_between(start_date="-3y", end_date="-3m")
 
        role = roles[0] if i == 0 else random.choice(roles[1:])
 
        if i == 0:
            salary = round(random.uniform(5000, 20000), 2)
        else:
            salary = round(random.uniform(1000, 5000), 2)
 
        is_active = random.choices([1, 0], weights=[95, 5])[0]
 
        cur.execute("""
            insert into employees (first_name, last_name, hire_date, email,
                                   role, salary, department_id, is_active)
            values (%s, %s, %s, %s, %s, %s, %s, %s)
        """, (fn, ln, str(hire_date), email, role, salary, dept_id, is_active))
 
conn.commit()
 

cur.execute("select employee_id, department_id from employees")
rows = cur.fetchall()
EMPS_BY_DEPT = {}
for eid, did in rows:
    for dname, did2 in DEPT_MAP.items():
        if did2 == did:
            EMPS_BY_DEPT.setdefault(dname, []).append(eid)
            break
 
for dept_name in DEPT_NAMES:
    head_id = EMPS_BY_DEPT[dept_name][0]
    cur.execute("update departments set head = %s where department_id = %s",
                (head_id, DEPT_MAP[dept_name]))
 
conn.commit()
print(f"employees: {emp_counter}")
 
 

 #suuppliers
SUPPLIER_COUNTRIES = {
    "Ukraine": ["Kyiv", "Dnipro", "Lviv"],
    "Czech Republic": ["Prague"],
    "Poland": ["Warsaw"],
    "Turkey": ["Istanbul"],
    "United Kingdom": ["London"],
    "China": ["Beijing", "Shanghai", "Shenzhen", "Chengdu"]
}
 
SUPPLIER_SUFFIXES = ["Defence", "Aerospace", "Tech", "Systems",
                     "Industries", "Components", "Engineering"]
 
NUM_SUPPLIERS = 70
 
for _ in range(NUM_SUPPLIERS):
    name = fake.company() + " " + random.choice(SUPPLIER_SUFFIXES)
    country = random.choice(list(SUPPLIER_COUNTRIES.keys()))
    city = random.choice(SUPPLIER_COUNTRIES[country])
    contact_name = fake.name()
    contact_email = f"sales{random.randint(1,999)}@{fake.domain_name()}"
    phone = fake.phone_number()[:20]
 
    cur.execute("""
        insert into suppliers (name, contact_name, contact_email, phone, country, city)
        values (%s, %s, %s, %s, %s, %s)
    """, (name, contact_name, contact_email, phone, country, city))
 
conn.commit()
supplier_ids = list(range(1, NUM_SUPPLIERS + 1))
print(f"suppliers: {NUM_SUPPLIERS}")
 
 

#components 
COMPONENT_CATALOG = {
    "Propulsion":   ["Jet engine", "Motor assembly", "Fuel pump", "Turbine blade set",
                     "Exhaust nozzle", "Fuel injector", "Thrust vectoring unit"],
    "Avionics":     ["Navigation module", "Flight controller", "GPS receiver",
                     "IMU sensor", "Altimeter", "Radar module", "LIDAR unit"],
    "Airframe":     ["Fuselage section", "Wing assembly", "Tail fin", "Nose cone",
                     "Canard set", "Composite panel", "Structural bracket"],
    "Warhead":      ["Warhead casing", "Detonator assembly", "Guidance fuse",
                     "Payload adapter", "Fragmentation sleeve"],
    "Electronics":  ["Wiring harness", "Power supply unit", "Antenna module",
                     "Camera module", "Radio transceiver", "Battery pack"],
    "Software":     ["Firmware license", "Navigation software", "Encryption module",
                     "Telemetry package", "Mission planning suite"],
}
 
comp_count = 0
 
for category, items in COMPONENT_CATALOG.items():
    for item_name in items:
        price = round(random.uniform(10, 250000), 2)
        sup_id = random.choice(supplier_ids)
        description = fake.sentence(nb_words=10)
 
        cur.execute("""
            insert into components (name, category, description, price, supplier_id)
            values (%s, %s, %s, %s, %s)
        """, (item_name, category, description, price, sup_id))
        comp_count += 1
 
conn.commit()
component_ids = list(range(1, comp_count + 1))
print(f"components: {comp_count}")
 
 

#purchase orders 
NUM_POS = 5000
PO_STATUSES = ["delivered", "shipped", "ordered", "cancelled", "partially_delivered"]
PO_STATUS_WEIGHTS = [75, 5, 10, 2, 8]

procurement_emps = EMPS_BY_DEPT["Procurement"]
 
for _ in range(NUM_POS):
    comp_id = random.choice(component_ids)
 
    cur.execute("select price from components where component_id = %s", (comp_id,))
    comp_price = cur.fetchone()[0]
 
    quantity = random.randint(1, 5000)
    order_date = fake.date_between(start_date="-2y", end_date="-7d")
    delivery_date = order_date + timedelta(days=random.randint(7, 180))
    status = random.choices(PO_STATUSES, weights=PO_STATUS_WEIGHTS)[0]
    responsible = random.choice(procurement_emps)
 
    cur.execute("""
        insert into purchase_orders (component_id, quantity, order_date,
                                     delivery_date, status, responsible)
        values (%s, %s, %s, %s, %s, %s)
    """, (comp_id, quantity, str(order_date), str(delivery_date),
          status, responsible))
 
conn.commit()
print(f"purchase_orders: {NUM_POS}")
 
 
 



#contractors
CONTRACTORS = {
    "government": {
        "Ukraine": ["UA Ministry of Defence", "UA National Guard", "UA State Border Guard"],
        "Poland": ["Polish MOD", "Polish Armed Forces Command"],
        "Estonia": ["Estonian Defence Forces"],
        "Latvia": ["Latvian National Armed Forces"],
        "Lithuania": ["Lithuanian Armed Forces"],
        "Czech Republic": ["Czech MOD"],
    },
    "allied_military": {
        "United Kingdom": ["UK MOD", "UK Royal Air Force"],
        "France": ["French DGA", "French Air and Space Force"],
        "Germany": ["German Bundeswehr Procurement"],
        "Denmark": ["Danish Defence Acquisition"],
        "Norway": ["Norwegian Defence Materiel Agency"],
        "Sweden": ["Swedish FMV"],
        "Finland": ["Finnish Defence Forces Logistics"],
    },
    "export_client": {
        "Turkey": ["Turkish SSB"],
        "South Korea": ["DAPA South Korea"],
        "USA": ["US SOCOM", "US DARPA"],
        "Israel": ["Israeli MOD Procurement"],
        "Japan": ["Japan ATLA"],
        "Australia": ["Australian CASG"],
    },
}
 
contractor_count = 0
 
for client_type, countries in CONTRACTORS.items():
    for country, names in countries.items():
        for name in names:
            contact_name = fake.name()
            contact_email = f"procurement{random.randint(1, 9999)}@{fake.domain_name()}"
            phone = fake.phone_number()[:20]
 
            cur.execute("""
                insert into contractors (name, client_type, contact_name,
                                         contact_email, phone, country)
                values (%s, %s, %s, %s, %s, %s)
            """, (name, client_type, contact_name, contact_email, phone, country))
            contractor_count += 1
 
conn.commit()
 
cur.execute("select contractor_id from contractors")
contractor_ids = [row[0] for row in cur.fetchall()]
print(f"contractors: {contractor_count}")
 
 
#prod lines
PRODUCTION_LINES = [
    ("UAV Assembly Line 1", "Kyiv Plant A"),
    ("UAV Assembly Line 2", "Kyiv Plant B"),
    ("Missile Assembly Line 1", "Western Facility"),
    ("Missile Assembly Line 2", "Western Facility"),
    ("Avionics Integration Bay", "Kyiv Plant A"),
    ("Propulsion Test Line", "Western Facility"),
    ("Final Assembly & QC", "Kyiv Plant B"),
    ("Prototype Workshop", "Western Facility"),
]
 
production_emps = EMPS_BY_DEPT["Production"]
 
for name, facility in PRODUCTION_LINES:
    supervisor = random.choice(production_emps)
    cur.execute("""
        insert into production_lines (name, facility, supervisor)
        values (%s, %s, %s)
    """, (name, facility, supervisor))
 
conn.commit()
 
cur.execute("select line_id from production_lines")
line_ids = [row[0] for row in cur.fetchall()]
print(f"production_lines: {len(PRODUCTION_LINES)}")
 
 

 #products
PRODUCTS = [
    ("FP-1 Deep Strike UAV", "FP-1", "deep_strike_uav",
     "Unmanned aerial vehicle for deep-strike missions beyond the front line, capable of hitting strategically important targets at significant range.",
     1000, 180, 150000, 95000, "2024-06-01"),
    ("FP-2 Front Strike UAV", "FP-2", "front_strike_uav",
     "Tactical UAV for front-line strike operations with high-precision targeting at ranges up to 200 km.",
     200, 150, 85000, 52000, "2025-01-15"),
    ("FP-5 Flamingo", "FP-5", "cruise_missile",
     "Long-range ground-launched cruise missile with operational range up to 3000 km, subsonic flight, advanced guidance and high-precision strike capability.",
     3000, 250, 500000, 320000, "2025-08-25"),
    ("FP-7 Tactical Ballistic Missile", "FP-7", "tactical_ballistic_missile",
     "Ground-launched tactical ballistic missile with max range 200 km, max speed 1500 m/s, flight time up to 250 seconds.",
     200, 1500, 750000, 480000, "2026-03-01"),
    ("FP-9 Extended Range Ballistic Missile", "FP-9", "ballistic_missile",
     "Ballistic missile with extended range up to 855 km, capable of deep rear strikes at max speed approximately 2200 m/s.",
     855, 2200, 1200000, 780000, "2026-03-01"),
]
 

uav_lines = [lid for lid, (n, _) in zip(line_ids, PRODUCTION_LINES) if "UAV" in n]
missile_lines = [lid for lid, (n, _) in zip(line_ids, PRODUCTION_LINES) if "Missile" in n]
 
for name, code, pclass, desc, rng, spd, price, cost, created in PRODUCTS:
    if "uav" in pclass:
        pl_id = random.choice(uav_lines) if uav_lines else random.choice(line_ids)
    else:
        pl_id = random.choice(missile_lines) if missile_lines else random.choice(line_ids)
 
    cur.execute("""
        insert into products (name, product_code, product_class, description,
                              max_range_km, max_speed_ms, base_unit_price,
                              unit_cost, is_active, created_at, production_line_id)
        values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, (name, code, pclass, desc, rng, spd, price, cost, 1, created, pl_id))
 
conn.commit()
 
cur.execute("select product_id, product_code, base_unit_price from products")
prod_rows = cur.fetchall()
PRODUCT_MAP = {code: (pid, float(price)) for pid, code, price in prod_rows}
product_ids = [pid for pid, _, _ in prod_rows]
PRODUCT_CODES = list(PRODUCT_MAP.keys())
print(f"products: {len(PRODUCTS)}")
 
 
#sales
NUM_SALES = 3000
SALES_STATUSES = ["active", "fulfilled", "draft", "terminated"]
SALES_STATUS_W = [5, 85, 5, 5]
CURRENCIES = ["USD"]
 
sales_emps = EMPS_BY_DEPT["Sales"]
 
for _ in range(NUM_SALES):
    ctr_id = random.choice(contractor_ids)
    product_code = random.choice(PRODUCT_CODES)
    prod_id, base_price = PRODUCT_MAP[product_code]
 
    qty = random.randint(1, 50)
    if product_code in ("FP-5"):
        qty = random.randint(1, 10)
    if product_code in ("FP-7", "FP-9"):
        qty = random.randint(1, 3)

    unit_val = base_price * random.uniform(0.95, 1.05)
    total_price = round(qty * unit_val, 2)
    currency = random.choice(CURRENCIES)
 
    order_date = fake.date_between(start_date="-2y", end_date="-1d")
    delivery_deadline = order_date + timedelta(days=random.randint(60, 365))
    status = random.choices(SALES_STATUSES, weights=SALES_STATUS_W)[0]
 
    if status == "fulfilled":
        actual_delivery = delivery_deadline + timedelta(days=random.randint(-14, 30))
    else:
        actual_delivery = None
 
    responsible = random.choice(sales_emps)
 
    cur.execute("""
        insert into sales_orders (contractor_id, quantity, total_price, currency,
                                   order_date, delivery_deadline, actual_delivery_date,
                                   status, responsible, product_id)
        values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, (ctr_id, qty, total_price, currency, str(order_date),
          str(delivery_deadline), str(actual_delivery) if actual_delivery else None,
          status, responsible, prod_id))
 
conn.commit()
 
cur.execute("select order_id, contractor_id, total_price, currency, order_date, status from sales_orders")
sales_rows = cur.fetchall()
print(f"sales_orders: {NUM_SALES}")
 
 
#prod orders
NUM_PROD = 300
PROD_STATUSES = ["completed", "in_progress", "planned", "cancelled"]
PROD_STATUS_W = [50, 25, 15, 10]
 
for _ in range(NUM_PROD):
    product_code = random.choice(PRODUCT_CODES)
    prod_id, _ = PRODUCT_MAP[product_code]
 
    qty = random.randint(1, 50)
    if product_code in ("FP-5", "FP-7", "FP-9"):
        qty = random.randint(1, 3)
 
    start_date = fake.date_between(start_date="-540d", end_date="-14d")
    status = random.choices(PROD_STATUSES, weights=PROD_STATUS_W)[0]
    duration = random.randint(14, 120)
    end_date = start_date + timedelta(days=duration) if status == "completed" else None
 
    pl_id = random.choice(line_ids)
    responsible = random.choice(production_emps)
 
    cur.execute("""
        insert into production_orders (product_id, production_line_id, quantity,
                                       start_date, end_date, status, responsible)
        values (%s, %s, %s, %s, %s, %s, %s)
    """, (prod_id, pl_id, qty, str(start_date),
          str(end_date) if end_date else None,
          status, responsible))
 
conn.commit()
 
cur.execute("select prod_order_id, start_date, end_date, status from production_orders")
prod_order_rows = cur.fetchall()
print(f"production_orders: {NUM_PROD}")
 

#prod status 
STAGE_NAMES = [
    "Material Preparation", "Airframe Assembly", "Avionics Integration",
    "Propulsion Install", "Wiring & Electronics", "Software Flash",
    "Quality Check", "Final Assembly", "Testing",
]
 
stage_count = 0
 
for prod_order_id, start_date, end_date, status in prod_order_rows:
    if status == "planned":
        stages = STAGE_NAMES[:2]
    elif status == "in_progress":
        stages = STAGE_NAMES[:random.randint(3, 7)]
    elif status == "completed":
        stages = STAGE_NAMES
    else:  # cancelled
        stages = STAGE_NAMES[:random.randint(1, 4)]
 
    duration_days = (end_date - start_date).days if end_date else random.randint(14, 60)
    stage_time = fake.date_time_between(
        start_date=start_date,
        end_date=start_date + timedelta(days=max(duration_days, 1))
    )
 
    for j, stage_name in enumerate(stages):
        s_start = stage_time
        hours = random.randint(4, 72)
        s_end = s_start + timedelta(hours=hours)
        stage_time = s_end + timedelta(hours=random.randint(1, 12))
 
        is_last = (j == len(stages) - 1)
 
        if status == "in_progress" and is_last:
            s_status = "in_progress"
            s_end = None
        elif status == "cancelled" and is_last:
            s_status = "cancelled"
            s_end = None
        elif status == "planned":
            s_status = "planned" if j > 0 else "in_progress"
            if j > 0:
                s_start = None
            s_end = None
        else:
            s_status = "completed"
 
        assigned = random.choice(production_emps)
 
        cur.execute("""
            insert into production_stages (prod_order_id, stage_name, start_time,
                                           end_time, responsible, status)
            values (%s, %s, %s, %s, %s, %s)
        """, (prod_order_id, stage_name,
              s_start.strftime("%Y-%m-%d %H:%M:%S") if s_start else None,
              s_end.strftime("%Y-%m-%d %H:%M:%S") if s_end else None,
              assigned, s_status))
        stage_count += 1
 
conn.commit()
print(f"production_stages: {stage_count}")
 
 

 

#budget
BUDGET_RANGES = {
    "Production": (2_000_000, 8_000_000),
    "Procurement": (5_000_000, 15_000_000),
    "R&D": (3_000_000, 10_000_000),
    "Sales": (500_000, 2_000_000),
    "Finance": (300_000, 1_000_000),
    "HR": (200_000, 800_000),
}
 
budget_count = 0
for dept_name, (lo, hi) in BUDGET_RANGES.items():
    dept_id = DEPT_MAP[dept_name]
    for year in [2024, 2025, 2026]:
        amount = round(random.uniform(lo, hi), 2)
        spent_pct = random.uniform(0.4, 0.95) if year <= 2025 else random.uniform(0.1, 0.4)
        spent = round(amount * spent_pct, 2)
 
        cur.execute("""
            insert into budgets (department_id, amount, spent, fiscal_year)
            values (%s, %s, %s, %s)
        """, (dept_id, amount, spent, year))
        budget_count += 1
 
conn.commit()
print(f"budgets: {budget_count}")
 
 
#invoices+payments
INV_STATUSES = ["paid", "issued", "overdue", "cancelled"]
INV_STATUS_W = [50, 25, 15, 10]
PAYMENT_METHODS = ["bank_transfer", "wire", "escrow", "letter_of_credit"]
 
invoice_count = 0
payment_count = 0
 
for order_id, ctr_id, total_price, currency, order_date, status in sales_rows:
    if status in ("draft", "terminated"):
        continue
 
    inv_status = random.choices(INV_STATUSES, weights=INV_STATUS_W)[0]
    issue_date = fake.date_between(start_date="-1y", end_date="today")
    due_date = issue_date + timedelta(days=random.choice([30, 45, 60, 90]))
 
    cur.execute("""
        insert into invoices (source_type, source_id, client_id, supplier_id,
                              amount, currency, issue_date, due_date, status)
        values (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, ("sales_order", order_id, ctr_id, None,
          float(total_price), currency, str(issue_date), str(due_date), inv_status))
 
    inv_id = cur.lastrowid
    invoice_count += 1
 
    if inv_status == "paid":
        pay_date = fake.date_between(start_date=issue_date, end_date=due_date + timedelta(days=15))
        method = random.choice(PAYMENT_METHODS)
        cur.execute("""
            insert into payments (invoice_id, amount, payment_date, method)
            values (%s, %s, %s, %s)
        """, (inv_id, float(total_price), str(pay_date), method))
        payment_count += 1

cur.execute("""
    select order_id, component_id, quantity, delivery_date
    from purchase_orders where status in ('delivered', 'partially_delivered')
""")
delivered_pos = cur.fetchall()
 
for po_id, comp_id, qty, delivery_date in delivered_pos:
    cur.execute("select price from components where component_id = %s", (comp_id,))
    comp_price = float(cur.fetchone()[0])
    amount = round(comp_price * qty, 2)
 
    cur.execute("select supplier_id from components where component_id = %s", (comp_id,))
    sup_id = cur.fetchone()[0]
 
    inv_status = random.choices(INV_STATUSES, weights=[55, 20, 15, 10])[0]
    issue_date = fake.date_between(start_date="-1y", end_date="today")
    due_date = issue_date + timedelta(days=random.choice([30, 45, 60]))
 
    cur.execute("""
        insert into invoices (source_type, source_id, client_id, supplier_id,
                              amount, currency, issue_date, due_date, status)
        values (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, ("purchase_order", po_id, None, sup_id,
          amount, "USD", str(issue_date), str(due_date), inv_status))
 
    inv_id = cur.lastrowid
    invoice_count += 1
 
    if inv_status == "paid":
        pay_date = fake.date_between(start_date=issue_date, end_date=due_date + timedelta(days=10))
        method = random.choice(PAYMENT_METHODS)
        cur.execute("""
            insert into payments (invoice_id, amount, payment_date, method)
            values (%s, %s, %s, %s)
        """, (inv_id, amount, str(pay_date), method))
        payment_count += 1
 
conn.commit()
print(f"invoices: {invoice_count}")
print(f"payments: {payment_count}")
 
 

#rnd
RD_PROJECT_NAMES = [
    ("FP-5 Flamingo Engine V2", "FP-5"),
    ("FP-5 Terrain Following Radar", "FP-5"),
    ("FP-7 Guidance System Upgrade", "FP-7"),
    ("FP-7 Mobile Launcher Design", "FP-7"),
    ("FP-9 Hypersonic Prototype", "FP-9"),
    ("Universal Encrypted Comms Module", None),
    ("AI Target Recognition System", None),
    ("Counter-EW Hardening Package", None)
]
 
RD_STATUSES = ["active", "completed", "on_hold", "cancelled"]
 
rd_emps = EMPS_BY_DEPT["R&D"]
 
for proj_name, product_code in RD_PROJECT_NAMES:
    prod_id = PRODUCT_MAP[product_code][0] if product_code else None
    lead = random.choice(rd_emps)
    budget = round(random.uniform(50_000, 2_000_000), 2)
    status = 'active'
 
    spent_pct = {
        "active": random.uniform(0.2, 0.8),
        "completed": random.uniform(0.85, 1.15),
        "on_hold": random.uniform(0.1, 0.5),
        "cancelled": random.uniform(0.05, 0.3),
    }[status]
    spent = round(budget * spent_pct, 2)
 
    start = fake.date_between(start_date="-3y", end_date="-3m")
    expected_end = start + timedelta(days=random.randint(90, 540))
 
    cur.execute("""
        insert into research_projects (name, product_id, lead_researcher,
                                       budget, spent, start_date, expected_end, status)
        values (%s, %s, %s, %s, %s, %s, %s, %s)
    """, (proj_name, prod_id, lead, budget, spent,
          str(start), str(expected_end), status))
 
conn.commit()
 
cur.execute("select project_id, start_date, expected_end, status from research_projects")
rd_rows = cur.fetchall()
print(f"research_projects: {len(RD_PROJECT_NAMES)}")
 
 
MILESTONE_TEMPLATES = [
    "Requirements & Specs Finalized",
    "Preliminary Design Review",
    "Prototype Build",
    "Component Testing",
    "Integration Testing",
    "Critical Design Review",
    "Field Trial",
    "Final Report & Handoff",
]
 
ms_count = 0
 
for project_id, start_date, expected_end, status in rd_rows:
    if status == "completed":
        milestones = MILESTONE_TEMPLATES
    elif status == "active":
        milestones = MILESTONE_TEMPLATES[:random.randint(4, 7)]
    elif status == "on_hold":
        milestones = MILESTONE_TEMPLATES[:random.randint(2, 4)]
    else:
        milestones = MILESTONE_TEMPLATES[:random.randint(1, 3)]
 
    ms_date = start_date + timedelta(days=random.randint(14, 30))
 
    for j, ms_name in enumerate(milestones):
        due = ms_date + timedelta(days=random.randint(20, 60))
        ms_date = due
        is_last = (j == len(milestones) - 1)
 
        if status == "completed":
            ms_status = "completed"
            comp_date = due + timedelta(days=random.randint(-7, 14))
        elif status == "active" and is_last:
            ms_status = "in_progress"
            comp_date = None
        elif status == "active":
            ms_status = "completed"
            comp_date = due + timedelta(days=random.randint(-5, 10))
        elif status == "on_hold":
            ms_status = "on_hold" if is_last else "completed"
            comp_date = None if is_last else due + timedelta(days=random.randint(0, 7))
        else:
            ms_status = "cancelled"
            comp_date = None
 
        description = fake.sentence(nb_words=8)
 
        cur.execute("""
            insert into milestones (project_id, name, description,
                                    due_date, completed_date, status)
            values (%s, %s, %s, %s, %s, %s)
        """, (project_id, ms_name, description,
              str(due), str(comp_date) if comp_date else None, ms_status))
        ms_count += 1
 
conn.commit()
print(f"milestones: {ms_count}")
 
 

print("\n" + "=" * 50)
print("  fire point — data generation complete")
print("=" * 50)
 
all_tables = [
    "departments", "employees", "suppliers", "components", "purchase_orders",
    "contractors", "sales_orders", "production_lines", "products",
    "production_orders", "production_stages", 
    "budgets", "invoices", "payments", "research_projects", "milestones",
]
 
for t in all_tables:
    cur.execute(f"select count(*) from {t}")
    count = cur.fetchone()[0]
    print(f"  {t:30s} {count:>6,} rows")
 
cur.close()
conn.close()
print("\ndone")