CREATE TABLE customers_raw (
    customer_id INT,
    name VARCHAR(50),
    city VARCHAR(50),
    age INT
);

INSERT INTO customers_raw (customer_id, name, city, age) VALUES
(1, 'Ravi', 'Hyderabad', 25),
(2, NULL, 'Chennai', 32),
(NULL, 'Arun', 'Hyderabad', 28),
(4, 'Meena', NULL, 30),
(4, 'Meena', NULL, 30),
(5, 'John', 'Bangalore', -5);


CREATE TABLE customers_clean AS
SELECT DISTINCT
    customer_id,
    IFNULL(name, 'Unknown') AS name,
    IFNULL(city, 'Unknown') AS city,
    age
FROM customers_raw
WHERE customer_id IS NOT NULL
AND age >= 0;

SELECT * FROM customers_clean;

SELECT COUNT(*) AS row_count_after_cleaning
FROM customers_clean;