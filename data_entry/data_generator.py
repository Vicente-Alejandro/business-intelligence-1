from faker import Faker
import random

# --- CONFIGURATION CONSTANTS ---
BATCH_SIZE = 1000
TOTAL_PRODUCT_LINES = 10
TOTAL_OFFICES = 50
TOTAL_PRODUCTS = 1000
TOTAL_EMPLOYEES = 1000
TOTAL_CUSTOMERS = 13000
TOTAL_PAYMENTS = 50000
TOTAL_ORDERS = 5000
TOTAL_ORDER_DETAILS = 10000
# -------------------------------

fake = Faker('en_US')

def populate_database(connection):
    """
    Receives an active MySQL connection, selects the transactional database,
    and inserts the generated mock data.
    """
    cursor = connection.cursor()
    cursor.execute("USE sales_system;")

    print("--- [2/3] Starting data generation and insertion into OLTP ---")

    print(f"Populating product_lines ({TOTAL_PRODUCT_LINES} records)...")
    lines = [(i, fake.word()[:50], fake.text()[:4000], f"<p>{fake.word()}</p>"[:200], fake.file_path()[:200]) for i in range(1, TOTAL_PRODUCT_LINES + 1)]
    cursor.executemany("INSERT INTO product_lines VALUES (%s, %s, %s, %s, %s)", lines)
    
    print(f"Populating offices ({TOTAL_OFFICES} records)...")
    office_ids = [f"OF-{i:03d}" for i in range(1, TOTAL_OFFICES + 1)]
    offices = [(id_of, fake.city()[:50], fake.phone_number()[:50], fake.street_address()[:50], 
                 fake.state()[:50], fake.country()[:50], fake.postcode()[:15], fake.word()[:10]) 
                for id_of in office_ids]
    cursor.executemany("INSERT INTO offices VALUES (%s, %s, %s, %s, %s, %s, %s, %s)", offices)

    print(f"Populating products ({TOTAL_PRODUCTS} records)...")
    product_ids = [f"PRD-{i:05d}" for i in range(1, TOTAL_PRODUCTS + 1)]
    products = []
    for id_prd in product_ids:
        price = round(random.uniform(10.0, 500.0), 2)
        products.append((id_prd, fake.catch_phrase()[:70], random.randint(1, TOTAL_PRODUCT_LINES), 
                          f"1:{random.choice([10, 12, 18, 24])}", random.randint(0, 1000), 
                          price, round(price * 1.2, 2)))
    cursor.executemany("INSERT INTO products VALUES (%s, %s, %s, %s, %s, %s, %s)", products)

    print(f"Populating employees ({TOTAL_EMPLOYEES} records)...")
    employee_docs = [fake.unique.random_int(min=10000000, max=99999999) for _ in range(TOTAL_EMPLOYEES)]
    employees = []
    for i, doc in enumerate(employee_docs):
        manager_id = None if i == 0 else random.choice(employee_docs[:i])
        employees.append((doc, fake.last_name()[:50], fake.first_name()[:50], 
                          fake.numerify('Ext-###'), fake.email()[:100], 
                          random.choice(office_ids), manager_id, fake.job()[:50]))
    for i in range(0, len(employees), BATCH_SIZE):
        cursor.executemany("INSERT INTO employees VALUES (%s, %s, %s, %s, %s, %s, %s, %s)", employees[i:i+BATCH_SIZE])

    print(f"Populating customers ({TOTAL_CUSTOMERS} records)...")
    customer_ids = list(range(1, TOTAL_CUSTOMERS + 1))
    customers = []
    for id_cli in customer_ids:
        customers.append((id_cli, fake.company()[:50], fake.last_name()[:50], fake.first_name()[:50], 
                          fake.phone_number()[:50], fake.street_address()[:50], fake.city()[:50], 
                          fake.state()[:50], fake.postcode()[:15], fake.country()[:50], 
                          random.choice(employee_docs), round(random.uniform(1000.0, 50000.0), 2)))
    for i in range(0, len(customers), BATCH_SIZE):
        cursor.executemany("INSERT INTO customers VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)", customers[i:i+BATCH_SIZE])

    print(f"Populating payments ({TOTAL_PAYMENTS} records)...")
    payments = [(random.choice(customer_ids), fake.unique.bothify(text='FAC-########'), 
              fake.date_between(start_date='-2y', end_date='today'), round(random.uniform(50.0, 5000.0), 2)) 
             for _ in range(TOTAL_PAYMENTS)]
    for i in range(0, len(payments), BATCH_SIZE):
        cursor.executemany("INSERT INTO payments VALUES (%s, %s, %s, %s)", payments[i:i+BATCH_SIZE])

    print(f"Populating orders ({TOTAL_ORDERS} records)...")
    order_ids = list(range(1, TOTAL_ORDERS + 1))
    orders = []
    for id_ord in order_ids:
        received_date = fake.date_between(start_date='-2y', end_date='today')
        orders.append((id_ord, received_date, fake.date_between(start_date=received_date, end_date='+30d'), 
                        fake.date_between(start_date=received_date, end_date='+15d'), 
                        random.choice(['Shipped', 'Processing', 'Cancelled'])[:15], 
                        fake.text()[:200], random.choice(customer_ids)))
    for i in range(0, len(orders), BATCH_SIZE):
        cursor.executemany("INSERT INTO orders VALUES (%s, %s, %s, %s, %s, %s, %s)", orders[i:i+BATCH_SIZE])

    print(f"Populating order_details ({TOTAL_ORDER_DETAILS} records)...")
    used_pk_keys = set()
    details_list = []
    while len(details_list) < TOTAL_ORDER_DETAILS:
        id_ord = random.choice(order_ids)
        id_prod = random.choice(product_ids)
        if (id_ord, id_prod) not in used_pk_keys:
            used_pk_keys.add((id_ord, id_prod))
            details_list.append((id_ord, id_prod, random.randint(1, 100), round(random.uniform(10.0, 500.0), 2), random.randint(1, 5)))
    for i in range(0, len(details_list), BATCH_SIZE):
        cursor.executemany("INSERT INTO order_details VALUES (%s, %s, %s, %s, %s)", details_list[i:i+BATCH_SIZE])

    connection.commit()
    cursor.close()
    print("Data inserted successfully.")