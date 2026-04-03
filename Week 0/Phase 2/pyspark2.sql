CREATE TABLE customers (
    customer_id INT,
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    email VARCHAR(100),
    phone_number VARCHAR(20),
    address VARCHAR(100),
    city VARCHAR(50),
    state VARCHAR(50),
    zip_code VARCHAR(10)
);
CREATE TABLE sales (
    sale_id INT,
    customer_id INT,
    product_id VARCHAR(20),
    sale_date DATE,
    quantity INT,
    total_amount INT
);
INSERT INTO customers VALUES
(1,'john','smith','johnsmith@gmail.com','11111','Add1','Hyderabad','TS','500001'),
(2,'emma','jones','emmajonnes@gmail.com','22222','Add2','Chennai','TN','600001'),
(3,'olivia','brown','oliviabrown@gmail.com','33333','Add3','Amaravati','AP','700001'),
(4,'johnson','liam','jhonsonliam@gmail.com','44444','Add4','Delhi','DL','800001');
INSERT INTO sales VALUES
(101,1,'P1','2025-01-01',2,000),
(102,1,'P2','2025-01-02',4,000),
(103,2,'P3','2025-01-03',5,000),
(104,3,'P1','2025-01-04',1,200);

SELECT customer_id, SUM(total_amount) AS totalamt
FROM sales
GROUP BY customer_id;

SELECT customer_id, SUM(total_amount) AS totalamt
FROM sales
GROUP BY customer_id
ORDER BY total_spend DESC
LIMIT 3;

SELECT *
FROM customers
LEFT JOIN sales
ON customers.customer_id=sales.customer_id
WHERE sales.customer_id IS NULL;

SELECT customers.city, SUM(sales.total_amount) AS revenuegen
FROM customers JOIN sales
ON customers.customer_id=sales.customer_id
GROUP BY customers.city;

SELECT customer_id, AVG(total_amount) AS avg_amt
FROM sales
GROUP BY customer_id;

SELECT customer_id,COUNT(*) AS orders
FROM sales
GROUP BY customer_id
HAVING COUNT(*)>1;

SELECT customer_id, SUM(total_amount) AS totalamt
FROM sales
GROUP BY customer_id
ORDER BY totalamt DESC;