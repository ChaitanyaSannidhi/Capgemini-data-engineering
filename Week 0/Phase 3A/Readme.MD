<!DOCTYPE html>
<html>
<head>
<title>Data Cleaning using PySpark and MySQL</title>
</head>

<body>

<h1>Data Cleaning Pipeline using PySpark and MySQL</h1>

<h2>Project Objective</h2>
<p>
The goal of this project is to clean messy data using PySpark and implement the same logic using MySQL.
Data cleaning ensures accuracy, consistency, and reliability before performing analysis or building pipelines.
</p>


<h2>Dataset Issues</h2>
<ul>
<li>Missing values in customer_id, name, city</li>
<li>Duplicate records</li>
<li>Invalid age values (negative numbers)</li>
<li>Incomplete customer information</li>
</ul>


<h2>Data Cleaning Rules Applied</h2>
<ul>
<li>Removed rows where customer_id is NULL</li>
<li>Replaced NULL values in name with 'Unknown'</li>
<li>Replaced NULL values in city with 'Unknown'</li>
<li>Removed duplicate records</li>
<li>Removed rows where age is negative</li>
</ul>


<h2>PySpark Implementation</h2>

<h3>Code</h3>

<pre>
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("CustomerSales").getOrCreate()

data = [
(1, "Ravi", "Hyderabad", 25),
(2, None, "Chennai", 32),
(None, "Arun", "Hyderabad", 28),
(4, "Meena", None, 30),
(4, "Meena", None, 30),
(5, "John", "Bangalore", -5)
]

columns = ["customer_id", "name", "city", "age"]

df = spark.createDataFrame(data, columns)

print("Row count before cleaning:", df.count())

df_clean = df.filter(col("customer_id").isNotNull())

df_clean = df_clean.fillna({
"name": "Unknown",
"city": "Unknown"
})

df_clean = df_clean.dropDuplicates()

df_clean = df_clean.filter(col("age") >= 0)

print("Cleaned Data")

df_clean.show()

print("Row count after cleaning:", df_clean.count())
</pre>



<h2>MySQL Equivalent Implementation</h2>

<h3>Create Table</h3>

<pre>
CREATE TABLE customers_raw (
customer_id INT,
name VARCHAR(50),
city VARCHAR(50),
age INT
);
</pre>


<h3>Insert Data</h3>

<pre>
INSERT INTO customers_raw VALUES
(1, 'Ravi', 'Hyderabad', 25),
(2, NULL, 'Chennai', 32),
(NULL, 'Arun', 'Hyderabad', 28),
(4, 'Meena', NULL, 30),
(4, 'Meena', NULL, 30),
(5, 'John', 'Bangalore', -5);
</pre>


<h3>Row Count Before Cleaning</h3>

<pre>
SELECT COUNT(*) FROM customers_raw;
</pre>


<h3>Cleaning Query</h3>

<pre>
CREATE TABLE customers_clean AS

SELECT DISTINCT
customer_id,
IFNULL(name,'Unknown') AS name,
IFNULL(city,'Unknown') AS city,
age

FROM customers_raw

WHERE customer_id IS NOT NULL
AND age >= 0;
</pre>


<h3>View Cleaned Data</h3>

<pre>
SELECT * FROM customers_clean;
</pre>


<h3>Row Count After Cleaning</h3>

<pre>
SELECT COUNT(*) FROM customers_clean;
</pre>



<h2>PySpark vs MySQL Mapping</h2>

<pre>
dropna()          -> WHERE column IS NOT NULL
fillna()          -> IFNULL()
dropDuplicates()  -> SELECT DISTINCT
filter()          -> WHERE condition
count()           -> COUNT()
</pre>



<h2>Conclusion</h2>
<p>
Both PySpark and MySQL can be used to perform data cleaning effectively. 
PySpark is suitable for big data processing, while MySQL is useful for structured relational data.
Understanding both approaches helps in building flexible data engineering pipelines.
</p>


<h3>Author</h3>


</body>
</html>