-- 1. Create the analytical database (Data Warehouse)
CREATE DATABASE IF NOT EXISTS sales_system_dw;
USE sales_system_dw;

-- 2. Create Dimension: Office (The "Where")
CREATE TABLE dim_office (
    office_id VARCHAR(10) PRIMARY KEY,
    continent VARCHAR(10),
    country VARCHAR(50),
    city VARCHAR(50)
);

-- 3. Create Dimension: Customer (The "Who")
CREATE TABLE dim_customer (
    customer_id INT(11) PRIMARY KEY,
    company_name VARCHAR(50),
    country VARCHAR(50),
    city VARCHAR(50),
    credit_limit DOUBLE
);

-- 4. Create Dimension: Time (The "When")
-- Note: Using an INT (YYYYMMDD) as Primary Key is the standard in Business Intelligence.
CREATE TABLE dim_time (
    time_id INT PRIMARY KEY, 
    full_date DATE,
    year_val INT,
    quarter_val INT,
    month_val INT,
    day_val INT,
    day_of_week INT
);

-- 5. Create Fact Table: Payments (The Numeric Results)
CREATE TABLE fact_payments (
    fact_payment_id INT AUTO_INCREMENT PRIMARY KEY,
    invoice_number VARCHAR(50),
    customer_id INT(11),
    office_id VARCHAR(10),
    time_id INT,
    total_amount DOUBLE,
    
    -- Foreign Keys defining relationships to dimensions
    CONSTRAINT fk_fact_customer FOREIGN KEY (customer_id) REFERENCES dim_customer(customer_id),
    CONSTRAINT fk_fact_office FOREIGN KEY (office_id) REFERENCES dim_office(office_id),
    CONSTRAINT fk_fact_time FOREIGN KEY (time_id) REFERENCES dim_time(time_id)
);

-- ============================================================================
-- ETL / Data Population Phase
-- ============================================================================

-- 1. Populate dim_office
-- Extracting only necessary attributes for geographical grouping.
INSERT INTO sales_system_dw.dim_office (office_id, continent, country, city)
SELECT office_id, continent, country, city 
FROM sales_system.offices;

-- 2. Populate dim_customer
-- Selecting customer hierarchy data and credit limits.
INSERT INTO sales_system_dw.dim_customer (customer_id, company_name, country, city, credit_limit)
SELECT customer_id, company_name, country, city, credit_limit 
FROM sales_system.customers;

-- 3. Populate dim_time
-- Generating the time dimension by extracting unique dates from completed payments.
INSERT INTO sales_system_dw.dim_time (time_id, full_date, year_val, quarter_val, month_val, day_val, day_of_week)
SELECT DISTINCT
    CAST(DATE_FORMAT(payment_date, '%Y%m%d') AS UNSIGNED) AS time_id,
    payment_date,
    YEAR(payment_date),
    QUARTER(payment_date),
    MONTH(payment_date),
    DAY(payment_date),
    DAYOFWEEK(payment_date)
FROM sales_system.payments
WHERE payment_date IS NOT NULL;

-- 4. Populate fact_payments (Fact Table)
-- Critical transformation: Crossing payments with customers and employees to identify the originating office for each payment.
INSERT INTO sales_system_dw.fact_payments (invoice_number, customer_id, office_id, time_id, total_amount)
SELECT 
    p.invoice_number,
    p.customer_id,
    e.office_id,
    CAST(DATE_FORMAT(p.payment_date, '%Y%m%d') AS UNSIGNED) AS time_id,
    p.total_amount
FROM sales_system.payments p
JOIN sales_system.customers c ON p.customer_id = c.customer_id
JOIN sales_system.employees e ON c.sales_rep_employee_id = e.document_id;

-- Verification
SELECT * FROM fact_payments LIMIT 10;