## Starter code
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("practice").getOrCreate()

customers_data = [
    (1, "Ravi", "Hyderabad", 25),
    (2, "Sita", "Chennai", 32),
    (3, "Arun", "Hyderabad", 28),
    (4, "Meena", "Bengaluru", 35),
    (5, "Kiran", "Chennai", 22)
]

orders_data = [
    (101, 1, 2500, "2026-03-01"),
    (102, 2, 1800, "2026-03-02"),
    (103, 1, 3200, "2026-03-03"),
    (104, 3, 1500, "2026-03-04"),
    (105, 5, 2800, "2026-03-05")
]

customers = spark.createDataFrame(customers_data, ["customer_id","customer_name","city","age"])
orders = spark.createDataFrame(orders_data, ["order_id","customer_id","amount","order_date"])

-----QUERIES----
# Show all customers
customers.show()

# Show customers from Chennai
customers.filter(customers.city == "Chennai").show()

# Show customers with age > 25
customers.filter("age > 25").show()

# Show only customer_name and city
customers.select("customer_name", "city").show()

# Count customers city-wise
customers.groupBy("city").count().show()