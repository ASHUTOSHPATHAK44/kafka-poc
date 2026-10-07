-- =============================================================================
-- Source schema for the Debezium PostgreSQL connector.
-- Runs once, on first start of an empty data volume.
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS inventory;

CREATE TABLE inventory.customers (
    id          serial PRIMARY KEY,
    first_name  varchar(100) NOT NULL,
    last_name   varchar(100) NOT NULL,
    email       varchar(255) NOT NULL UNIQUE,
    updated_at  timestamptz  NOT NULL DEFAULT now()
);

CREATE TABLE inventory.orders (
    id           serial PRIMARY KEY,
    customer_id  int            NOT NULL REFERENCES inventory.customers (id),
    product      varchar(200)   NOT NULL,
    quantity     int            NOT NULL,
    amount       numeric(12, 2) NOT NULL,
    created_at   timestamptz    NOT NULL DEFAULT now()
);

-- Debezium needs the full "before" image to emit complete UPDATE/DELETE events.
ALTER TABLE inventory.customers REPLICA IDENTITY FULL;
ALTER TABLE inventory.orders    REPLICA IDENTITY FULL;

INSERT INTO inventory.customers (first_name, last_name, email) VALUES
    ('Asha',  'Patel',   'asha.patel@example.com'),
    ('Rohan', 'Mehta',   'rohan.mehta@example.com'),
    ('Li',    'Wei',     'li.wei@example.com'),
    ('Maria', 'Gonzalez','maria.gonzalez@example.com');

INSERT INTO inventory.orders (customer_id, product, quantity, amount) VALUES
    (1, 'Mechanical Keyboard', 1,  129.00),
    (2, 'USB-C Dock',          2,  310.50),
    (3, '27in Monitor',        1,  289.99),
    (1, 'Desk Mat',            3,   74.25);

-- Publication used by the pgoutput logical decoding plugin.
CREATE PUBLICATION dbz_publication FOR TABLE inventory.customers, inventory.orders;

-- =============================================================================
-- Dedicated database for the Iceberg REST catalog's JDBC metadata.
-- Kept separate from the CDC 'inventory' data. Only runs on a FRESH volume;
-- for an existing volume, create it manually (see chat commands).
-- =============================================================================
CREATE ROLE iceberg WITH LOGIN PASSWORD 'iceberg';
CREATE DATABASE iceberg_catalog OWNER iceberg;
