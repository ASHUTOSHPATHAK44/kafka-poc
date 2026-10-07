-- =============================================================================
-- One tick of the data generator: insert a random customer and a random order,
-- and PRINT the rows that were inserted (so the Airflow log is readable).
--
-- Uses only data already in the DB (picks a random existing customer for the
-- order's foreign key) so it never violates the orders -> customers FK.
-- =============================================================================

\echo '=== INSERTED CUSTOMER ==='
INSERT INTO inventory.customers (first_name, last_name, email)
SELECT
    (ARRAY['Asha','Rohan','Li','Maria','Sam','Nina','Omar','Priya','Leo','Zoe'])[floor(random()*10)+1],
    (ARRAY['Patel','Mehta','Wei','Gonzalez','Khan','Silva','Nguyen','Brown','Kim','Rossi'])[floor(random()*10)+1],
    'user_' || floor(random()*1000000)::bigint || '@example.com'
RETURNING id, first_name, last_name, email;

\echo '=== INSERTED ORDER ==='
INSERT INTO inventory.orders (customer_id, product, quantity, amount)
SELECT
    c.id,
    (ARRAY['Keyboard','USB-C Dock','Monitor','Desk Mat','Mouse','Webcam','Headset','Laptop Stand','Cable','Hub'])[floor(random()*10)+1],
    floor(random()*5)+1,
    round((random()*500 + 10)::numeric, 2)
FROM inventory.customers c
ORDER BY random()
LIMIT 1
RETURNING id, customer_id, product, quantity, amount;

\echo '=== TOTALS (customers | orders) ==='
SELECT
    (SELECT count(*) FROM inventory.customers) AS customers,
    (SELECT count(*) FROM inventory.orders)    AS orders;
