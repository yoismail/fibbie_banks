CREATE SCHEMA IF NOT EXISTS analytics;

-- Drop in correct order (fact first, then dims)
DROP TABLE IF EXISTS analytics.fact_transactions;
DROP TABLE IF EXISTS analytics.dim_transaction;
DROP TABLE IF EXISTS analytics.dim_employee;
DROP TABLE IF EXISTS analytics.dim_customer;
DROP TABLE IF EXISTS analytics.dim_date;

-- Dimension: dim_date
CREATE TABLE IF NOT EXISTS analytics.dim_date (
    date_key        INT PRIMARY KEY,               -- Format: YYYYMMDD
    full_date       DATE NOT NULL UNIQUE,
    year            INT NOT NULL,
    month           INT NOT NULL,
    month_name      VARCHAR(20) NOT NULL,
    quarter         INT NOT NULL,
    day_of_month    INT NOT NULL,
    day_name        VARCHAR(20) NOT NULL,
    is_weekend      BOOLEAN NOT NULL
);

-- Dimension: dim_customer
CREATE TABLE IF NOT EXISTS analytics.dim_customer (
    customer_id      VARCHAR(64) PRIMARY KEY,
    customer_name    VARCHAR(255),
    customer_address TEXT,
    customer_city    VARCHAR(100),
    customer_state   VARCHAR(100),
    customer_country VARCHAR(100),
    email            VARCHAR(255),
    phone_number     VARCHAR(100)
);

-- Dimension: dim_transaction
CREATE TABLE IF NOT EXISTS analytics.dim_transaction (
    transaction_id   VARCHAR(64) PRIMARY KEY,
    transaction_date TIMESTAMP NOT NULL,
    date_key         INT NOT NULL REFERENCES analytics.dim_date(date_key),
    transaction_type VARCHAR(50) NOT NULL,
    amount           NUMERIC(18,2) NOT NULL
);

-- Dimension: dim_employee
CREATE TABLE IF NOT EXISTS analytics.dim_employee (
    employee_id      VARCHAR(64) PRIMARY KEY,
    company          VARCHAR(255),
    job_title        VARCHAR(255),
    gender           VARCHAR(50),
    marital_status   VARCHAR(50)
);


-- Fact Table: fact_transactions
CREATE TABLE IF NOT EXISTS analytics.fact_transactions (
    -- Surrogate keys linking to dimensions
    transaction_id   VARCHAR(64) PRIMARY KEY REFERENCES analytics.dim_transaction(transaction_id),
    customer_id      VARCHAR(64) NOT NULL REFERENCES analytics.dim_customer(customer_id),
    employee_id      VARCHAR(64) NOT NULL REFERENCES analytics.dim_employee(employee_id),
    date_key         INT NOT NULL REFERENCES analytics.dim_date(date_key),

    -- Transaction attributes
    credit_card_number NUMERIC(50),   -- ✅ Changed from NUMERIC → VARCHAR to support masked/long values
    iban               VARCHAR(80),
    currency_code     VARCHAR(10),
    random_number     NUMERIC(18,4),
    category          VARCHAR(100),
    "group"           VARCHAR(100),  -- quoted because GROUP is reserved word
    is_active         VARCHAR(10),        -- ✅ Changed from VARCHAR → BOOLEAN (cleaner type)
    last_updated      TIMESTAMP,
    description       TEXT
);

-- Indexes for performance
CREATE INDEX IF NOT EXISTS idx_fact_transaction_id ON analytics.fact_transactions (transaction_id);
CREATE INDEX IF NOT EXISTS idx_fact_customer_id   ON analytics.fact_transactions (customer_id);
CREATE INDEX IF NOT EXISTS idx_fact_employee_id   ON analytics.fact_transactions (employee_id);
CREATE INDEX IF NOT EXISTS idx_fact_date_key      ON analytics.fact_transactions (date_key);
CREATE INDEX IF NOT EXISTS idx_fact_last_updated  ON analytics.fact_transactions (last_updated);
CREATE INDEX IF NOT EXISTS idx_dim_date_full_date ON analytics.dim_date(full_date);
CREATE INDEX IF NOT EXISTS idx_dim_trans_date_key ON analytics.dim_transaction(date_key);

-- To run this script, use the following command in the terminal
--  psql -U postgres -d fibbie_bank_db -f sql\create_tables.sql