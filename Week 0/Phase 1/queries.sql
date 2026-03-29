-- Create customers table
CREATE TABLE customers (
    customer_id INT,
    customer_name VARCHAR(50),
    city VARCHAR(50),
    age INT
);

-- Insert sample data into customers
INSERT INTO customers (customer_id, customer_name, city, age) VALUES
(1, 'Ravi', 'Hyderabad', 25),
(2, 'Sita', 'Chennai', 32),
(3, 'Arun', 'Hyderabad', 28),
(4, 'Meena', 'Bengaluru', 35),
(5, 'Kiran', 'Chennai', 22);

-- Create orders table
CREATE TABLE orders (
    order_id INT,
    customer_id INT,
    amount INT,
    order_date DATE
);

-- Insert sample data into orders
INSERT INTO orders (order_id, customer_id, amount, order_date) VALUES
(101, 1, 2500, '2026-03-01'),
(102, 2, 1800, '2026-03-02'),
(103, 1, 3200, '2026-03-03'),
(104, 3, 1500, '2026-03-04'),
(105, 5, 2800, '2026-03-05');

---QUERIES---

-- Show all customers
Select * from customers;

-- Customers from Chennai
Select * from customers where city = 'Chennai';

-- Customers with age > 25
Select * from customers where age > 25;

-- Show only customer_name and city
Select customer_name, city from customers;

-- Count customers city wise
Select city, count(*) as total_customers from customers group by city;