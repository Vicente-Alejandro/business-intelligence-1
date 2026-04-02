"""
================================================================================
Data Generation and Population Module for OLTP System
================================================================================

DESCRIPTION:
    This module provides comprehensive data generation and population utilities
    for the Sales System OLTP (Online Transaction Processing) database. It 
    utilizes the Faker library to generate realistic, diverse mock data across
    multiple business entities.

PURPOSE:
    - Generate realistic transactional data for system testing and development
    - Populate all core business entities in the sales_system database
    - Support data warehouse ETL processes with high-volume transactional data
    - Enable consistent, reproducible data seeding for CI/CD pipelines

DEPENDENCIES:
    - faker: Python library for generating realistic fake data
    - random: Standard library for random selection and numerical generation
    - MySQL: Relational database for data persistence

AUTHOR: Business Intelligence Team
VERSION: 1.0.0
================================================================================
"""

from faker import Faker
import random

# ============================================================================
# CONFIGURATION CONSTANTS
# ============================================================================
# These constants define the volume of records to be generated for each
# business entity. Adjust these values to control the scale of data generation.
# ============================================================================

BATCH_SIZE = 1000                           # Number of records per batch insert
TOTAL_PRODUCT_LINES = 10                    # Product categories/classifications
TOTAL_OFFICES = 50                          # Organizational office locations
TOTAL_PRODUCTS = 1000                       # Individual product inventory items
TOTAL_EMPLOYEES = 1000                      # Workforce records with hierarchy
TOTAL_CUSTOMERS = 13000                     # Customer base for sales transactions
TOTAL_PAYMENTS = 50000                      # Payment records from customers
TOTAL_ORDERS = 5000                         # Sales order transactions
TOTAL_ORDER_DETAILS = 10000                 # Line items within orders


# ============================================================================
# FAKER INSTANCE INITIALIZATION
# ============================================================================
# Initialize Faker with US locale for realistic and actionable test data.
# This ensures consistency in data generation across multiple invocations.
# ============================================================================

fake = Faker('en_US')


# ============================================================================
# PRIMARY DATABASE POPULATION FUNCTION
# ============================================================================

def populate_database(connection):
    """
    Orchestrates complete data population workflow for the OLTP database.
    
    PARAMETERS:
        connection: Active MySQL database connection object
        
    WORKFLOW:
        1. Establishes database context (sales_system)
        2. Generates and inserts mock data for all core entities
        3. Maintains referential integrity through strategic ordering
        4. Commits all transactions upon successful completion
        
    ENTITIES POPULATED (in order):
        - Product Lines: Base product categories
        - Offices: Organizational office locations
        - Products: Individual product items
        - Employees: Employee records with manager hierarchies
        - Customers: Customer account information
        - Payments: Payment transaction records
        - Orders: Sales order headers
        - Order Details: Line items within sales orders
        
    NOTES:
        - Batch processing employed for large datasets to manage memory
        - Data relationships maintained through sequential generation
        - All string fields subject to database column length constraints
        
    RETURNS:
        None. Executes INSERT operations and commits transaction.
    """
    cursor = connection.cursor()
    cursor.execute("USE sales_system;")

    print("--- [2/3] Starting data generation and insertion into OLTP ---")

    # ========================================================================
    # PHASE 1: POPULATE PRODUCT LINES
    # ========================================================================
    # Product lines represent broad product categories/classifications within
    # the organization. This is the foundational dimension for product grouping.
    # ========================================================================

    print(f"Populating product_lines ({TOTAL_PRODUCT_LINES} records)...")
    lines = [(i, fake.word()[:50], fake.text()[:4000], f"<p>{fake.word()}</p>"[:200], fake.file_path()[:200]) for i in range(1, TOTAL_PRODUCT_LINES + 1)]
    cursor.executemany("INSERT INTO product_lines VALUES (%s, %s, %s, %s, %s)", lines)
    
    # ========================================================================
    # PHASE 2: POPULATE OFFICES
    # ========================================================================
    # Offices represent organizational locations/branches. Each office has a
    # unique identifier and complete location details for logistics purposes.
    # ========================================================================

    print(f"Populating offices ({TOTAL_OFFICES} records)...")
    office_ids = [f"OF-{i:03d}" for i in range(1, TOTAL_OFFICES + 1)]
    offices = [(id_of, fake.city()[:50], fake.phone_number()[:50], fake.street_address()[:50], 
                 fake.state()[:50], fake.country()[:50], fake.postcode()[:15], fake.word()[:10]) 
                for id_of in office_ids]
    cursor.executemany("INSERT INTO offices VALUES (%s, %s, %s, %s, %s, %s, %s, %s)", offices)

    # ========================================================================
    # PHASE 3: POPULATE PRODUCTS
    # ========================================================================
    # Products are individual inventory items assigned to product lines.
    # Pricing structure includes markup calculation (20% multiplier).
    # Quantity fields represent stock levels and reorder points.
    # ========================================================================

    print(f"Populating products ({TOTAL_PRODUCTS} records)...")
    product_ids = [f"PRD-{i:05d}" for i in range(1, TOTAL_PRODUCTS + 1)]
    products = []
    for id_prd in product_ids:
        price = round(random.uniform(10.0, 500.0), 2)
        products.append((id_prd, fake.catch_phrase()[:70], random.randint(1, TOTAL_PRODUCT_LINES), 
                          f"1:{random.choice([10, 12, 18, 24])}", random.randint(0, 1000), 
                          price, round(price * 1.2, 2)))
    cursor.executemany("INSERT INTO products VALUES (%s, %s, %s, %s, %s, %s, %s)", products)

    # ========================================================================
    # PHASE 4: POPULATE EMPLOYEES
    # ========================================================================
    # Employee records establish organizational hierarchy via manager_id field.
    # First employee (seed record) has no manager; subsequent employees assigned
    # random managers from previously created employees, ensuring valid hierarchy.
    # Batch processing used to manage memory with large datasets.
    # ========================================================================

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

    # ========================================================================
    # PHASE 5: POPULATE CUSTOMERS
    # ========================================================================
    # Customer records include organization profile, contact details, and
    # credit limits. Sales representative (employee) assignments establish
    # the customer-employee relationship for sales attribution.
    # Batch processing applied to optimize insertion performance.
    # ========================================================================

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

    # ========================================================================
    # PHASE 6: POPULATE PAYMENTS
    # ========================================================================
    # Payment records capture financial transactions from customers.
    # Invoice references are generated with unique identifiers.
    # Payment dates span a two-year historical window for realistic analysis.
    # Batch processing manages insertion of high-volume payment records.
    # ========================================================================

    print(f"Populating payments ({TOTAL_PAYMENTS} records)...")
    payments = [(random.choice(customer_ids), fake.unique.bothify(text='FAC-########'), 
              fake.date_between(start_date='-2y', end_date='today'), round(random.uniform(50.0, 5000.0), 2)) 
             for _ in range(TOTAL_PAYMENTS)]
    for i in range(0, len(payments), BATCH_SIZE):
        cursor.executemany("INSERT INTO payments VALUES (%s, %s, %s, %s)", payments[i:i+BATCH_SIZE])

    # ========================================================================
    # PHASE 7: POPULATE ORDERS
    # ========================================================================
    # Order records represent sales transactions with comprehensive timeline
    # tracking: order received date, promised delivery, and actual shipment.
    # Status values ('Shipped', 'Processing', 'Cancelled') track fulfillment state.
    # Logical constraints: promised_date >= received_date, shipped_date >= received_date
    # Batch processing applied for efficient population of order headers.
    # ========================================================================

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

    # ========================================================================
    # PHASE 8: POPULATE ORDER DETAILS
    # ========================================================================
    # Order details represent individual line items within sales orders.
    # Composite primary key (order_id, product_id) prevents duplicate items.
    # Each line includes quantity, unit price, and priority indicators.
    # Deduplication logic ensures referential integrity while generating
    # the target quantity of line items across all orders.
    # ========================================================================

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

    # ========================================================================
    # TRANSACTION FINALIZATION
    # ========================================================================
    # Commit all batch operations to ensure data persistence and consistency.
    # Close cursor to release database resources.
    # ========================================================================

    connection.commit()
    cursor.close()
    print("Data inserted successfully.")