CREATE TABLE customers (
    customer_id INT,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    email VARCHAR(150),
    phone_number VARCHAR(20),
    address VARCHAR(200),
    city VARCHAR(100),
    state VARCHAR(50),
    zip_code VARCHAR(20)
);

CREATE TABLE sales (
    sale_id INT,
    customer_id INT,
    product_id INT,
    sale_date DATE,
    quantity INT,
    total_amount DOUBLE
);

INSERT INTO customers VALUES
    (1, 'John', 'Smith', 'john.smith@domain.com', '555-0001', '123 Elm St', 'Springfield', 'IL', '62701'),
    (2, 'Emma', 'Jones', 'emma.jones@webmail.com', '555-0002', '456 Oak St', 'Centerville', 'OH', '45459'),
    (3, 'Olivia', 'Brown', 'olivia.brown@outlook.com', '555-0003', '789 Pine St', 'Greenville', 'SC', '29601'),
    (4, 'Liam', 'Johnson', 'liam.johnson@mail.com', '555-0004', '321 Maple St', 'Austin', 'TX', '73301'),
    (5, 'Noah', 'Williams', 'noah.williams@mail.com', '555-0005', '654 Cedar St', 'Dallas', 'TX', '75001');

INSERT INTO sales VALUES
    (1, 1, 101, '2024-01-15', 2, 39.98),
    (2, 1, 102, '2024-01-20', 1, 29.99),
    (3, 2, 103, '2024-01-16', 1, 25.00),
    (4, 2, 104, '2024-01-22', 3, 89.97),
    (5, 3, 105, '2024-01-17', 2, 49.98),
    (6, 1, 106, '2024-01-25', 1, 19.99),
    (7, 4, 107, '2024-01-18', 2, 59.98),
    (8, 5, 108, '2024-01-19', 1, 15.00),
    (9, 5, 109, '2024-01-21', 2, 30.00),
    (10, 5, 110, '2024-01-23', 1, 20.00);

-- Task 1: Daily Sales

Select sale_date , Round(Sum(total_amount),2) as total_sales
From sales
Group by sale_date

-- Task 2: City-wise Revenue

Select city , Round(Sum(total_amount),2) as total_sales
From customers c
Join sales s on c.customer_id = s.customer_id 
Group by c.city

-- Task 3: Top 5 Customers

Select first_name , ROUND(SUM(s.total_amount), 2) AS total_spend
FROM customers c
JOIN sales s
    ON c.customer_id = s.customer_id
GROUP BY c.customer_id, c.first_name
ORDER BY total_spend DESC
LIMIT 5;

-- Task 4: Repeat Customers (>1 order)

SELECT 
    customer_id,
    COUNT(*) AS order_count
FROM sales
GROUP BY customer_id
HAVING COUNT(*) > 1;

-- Task 5: Customer Segmentation

SELECT 
    CONCAT(c.first_name, ' ', c.last_name) AS customer_name,
    ROUND(SUM(s.total_amount), 2) AS total_spend,
    CASE
        WHEN SUM(s.total_amount) > 10000 THEN 'Gold'
        WHEN SUM(s.total_amount) >= 5000 THEN 'Silver'
        ELSE 'Bronze'
    END AS segment
FROM customers c
JOIN sales s
    ON c.customer_id = s.customer_id
GROUP BY c.customer_id, c.first_name, c.last_name;


-- Task 6: Final Reporting Table
SELECT 
    CONCAT(c.first_name, ' ', c.last_name) AS customer_name,
    c.city,
    ROUND(SUM(s.total_amount), 2) AS total_spend,
    COUNT(*) AS order_count,
    CASE
        WHEN SUM(s.total_amount) > 10000 THEN 'Gold'
        WHEN SUM(s.total_amount) >= 5000 THEN 'Silver'
        ELSE 'Bronze'
    END AS segment
FROM customers c
JOIN sales s
    ON c.customer_id = s.customer_id
GROUP BY c.customer_id, c.first_name, c.last_name, c.city;