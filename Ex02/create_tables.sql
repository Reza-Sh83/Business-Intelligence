CREATE TABLE dim_date (
    date_sk       INT PRIMARY KEY,          -- YYYYMMDD
    full_date     DATE NOT NULL,
    year          INT NOT NULL,
    quarter       INT NOT NULL,
    month         INT NOT NULL,
    month_name    VARCHAR(20) NOT NULL,
    day_of_week   INT NOT NULL,            -- Sunday=0 .. Saturday=6
    is_weekend    BOOLEAN NOT NULL
);

CREATE TABLE dim_time (
    time_sk       INT PRIMARY KEY,          -- HHMM integer, e.g. 830
    time_of_day   TIME NOT NULL,
    hour          INT NOT NULL,
    minute        INT NOT NULL,
    hour_minute   VARCHAR(5) NOT NULL,     -- 'HH:MM'
    part_of_day   VARCHAR(20) NOT NULL     -- Morning, Afternoon, Evening, Night
);

CREATE TABLE dim_store (
    store_sk     SERIAL PRIMARY KEY,
    store_bk     VARCHAR(50) NOT NULL,     -- natural key from source
    store_name   VARCHAR(255),
    lat          DECIMAL(10,7),
    lon          DECIMAL(10,7),
    vendor_type  VARCHAR(100),
    valid_from   TIMESTAMP NOT NULL DEFAULT NOW(),
    valid_to     TIMESTAMP,                -- NULL for current version
    is_current   BOOLEAN NOT NULL DEFAULT TRUE
);

CREATE UNIQUE INDEX idx_store_bk_current ON dim_store(store_bk) WHERE is_current;
CREATE INDEX idx_store_bk ON dim_store(store_bk);

CREATE TABLE dim_product (
    product_sk   SERIAL PRIMARY KEY,
    product_bk   VARCHAR(50) NOT NULL,     -- natural key from source
    product_name VARCHAR(255),
    category     VARCHAR(100),
    price        DECIMAL(10,2),
    valid_from   TIMESTAMP NOT NULL DEFAULT NOW(),
    valid_to     TIMESTAMP,
    is_current   BOOLEAN NOT NULL DEFAULT TRUE
);
CREATE UNIQUE INDEX idx_product_bk_current ON dim_product(product_bk) WHERE is_current;
CREATE INDEX idx_product_bk ON dim_product(product_bk);

CREATE TABLE dim_customer (
    customer_sk  SERIAL PRIMARY KEY,
    customer_bk  INT NOT NULL UNIQUE       -- natural key from source
);

CREATE TABLE fact_sales (
    sale_sk        SERIAL PRIMARY KEY,
    ticket_item_id INT NOT NULL,           -- natural key of the line item (dedup)
    ticket_id      INT,                    -- optional original ticket ID
    date_sk        INT NOT NULL REFERENCES dim_date(date_sk),
    time_sk        INT NOT NULL REFERENCES dim_time(time_sk),
    store_sk       INT NOT NULL REFERENCES dim_store(store_sk),
    product_sk     INT NOT NULL REFERENCES dim_product(product_sk),
    customer_sk    INT NOT NULL REFERENCES dim_customer(customer_sk),
    quantity       INT NOT NULL,
    unit_price     DECIMAL(10,2) NOT NULL,
    line_amount    DECIMAL(10,2) NOT NULL,
    discount       DECIMAL(10,2) DEFAULT 0.00,
    created_at     TIMESTAMP DEFAULT NOW()
);

CREATE UNIQUE INDEX idx_fact_sales_ticket_item ON fact_sales(ticket_item_id);

CREATE INDEX idx_fact_date ON fact_sales(date_sk);
CREATE INDEX idx_fact_store ON fact_sales(store_sk);
CREATE INDEX idx_fact_product ON fact_sales(product_sk);
CREATE INDEX idx_fact_customer ON fact_sales(customer_sk);

CREATE TABLE etl_control (
    table_name        VARCHAR(100) PRIMARY KEY,
    last_load_ts      TIMESTAMP NOT NULL
);

INSERT INTO etl_control (table_name, last_load_ts)
VALUES ('fact_ticket_items', '1970-01-01 00:00:00')
ON CONFLICT (table_name) DO NOTHING;