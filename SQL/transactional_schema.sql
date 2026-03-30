DROP DATABASE IF EXISTS sales_system;
CREATE DATABASE sales_system;
USE sales_system;

-- 1. Table: product_lines 
CREATE TABLE product_lines (
    product_line_id INT(5) PRIMARY KEY, 
    line_name VARCHAR(50), 
    text_description VARCHAR(4000), 
    html_description VARCHAR(200), 
    image_path VARCHAR(200) 
);

-- 2. Table: offices 
CREATE TABLE offices (
    office_id VARCHAR(10) PRIMARY KEY,
    city VARCHAR(50), 
    phone VARCHAR(50), 
    address VARCHAR(50), 
    state VARCHAR(50), 
    country VARCHAR(50), 
    postal_code VARCHAR(15), 
    continent VARCHAR(10) 
);

-- 3. Table: products 
CREATE TABLE products (
    product_id VARCHAR(15) PRIMARY KEY, 
    product_name VARCHAR(70), 
    product_line_id INT(5), 
    scale VARCHAR(10), 
    quantity INT(6), 
    sale_price DOUBLE, 
    msrp DOUBLE, 
    CONSTRAINT fk_product_line FOREIGN KEY (product_line_id) 
        REFERENCES product_lines(product_line_id) 
);

-- 4. Table: employees 
CREATE TABLE employees (
    document_id INT(11) PRIMARY KEY,
    last_name VARCHAR(50), 
    first_name VARCHAR(50), 
    extension VARCHAR(10), 
    email VARCHAR(100), 
    office_id VARCHAR(10), 
    manager_id INT(11), 
    job_title VARCHAR(50), 
    CONSTRAINT fk_employee_office FOREIGN KEY (office_id) 
        REFERENCES offices(office_id), 
    CONSTRAINT fk_employee_manager FOREIGN KEY (manager_id) 
        REFERENCES employees(document_id) 
);

-- 5. Table: customers 
CREATE TABLE customers (
    customer_id INT(11) PRIMARY KEY, 
    company_name VARCHAR(50), 
    last_name VARCHAR(50),
    first_name VARCHAR(50), 
    phone VARCHAR(50), 
    address VARCHAR(50), 
    city VARCHAR(50), 
    state VARCHAR(50), 
    postal_code VARCHAR(15), 
    country VARCHAR(50), 
    sales_rep_employee_id INT(11),
    credit_limit DOUBLE, 
    CONSTRAINT fk_customer_employee FOREIGN KEY (sales_rep_employee_id) 
        REFERENCES employees(document_id)
);

-- 6. Table: payments 
CREATE TABLE payments (
    customer_id INT(11), 
    invoice_number VARCHAR(50) PRIMARY KEY, 
    payment_date DATE, 
    total_amount DOUBLE,
    CONSTRAINT fk_payment_customer FOREIGN KEY (customer_id) 
        REFERENCES customers(customer_id) 
);

-- 7. Table: orders 
CREATE TABLE orders (
    order_id INT(11) PRIMARY KEY,
    received_date DATE, 
    required_date DATE, 
    shipped_date DATE, 
    status VARCHAR(15), 
    comments TEXT, 
    customer_id INT(11), 
    CONSTRAINT fk_order_customer FOREIGN KEY (customer_id) 
        REFERENCES customers(customer_id) 
);

-- 8. Table: order_details 
CREATE TABLE order_details (
    order_id INT(11), 
    product_id VARCHAR(15),
    quantity_ordered INT(11), 
    unit_price DOUBLE, 
    order_line_number INT(6), 
    PRIMARY KEY (order_id, product_id),
    CONSTRAINT fk_detail_order FOREIGN KEY (order_id) 
        REFERENCES orders(order_id),
    CONSTRAINT fk_detail_product FOREIGN KEY (product_id) 
        REFERENCES products(product_id)
);
