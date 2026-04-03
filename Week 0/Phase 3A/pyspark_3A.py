from pyspark.sql import SparkSession
from pyspark.sql.functions import sum,col
from pyspark.sql.functions import avg
from pyspark.sql.functions import count
spark=SparkSession.builder.appName("CustomerSales").getOrCreate()

customers = spark.read.option("header", "true").csv("/samples/customers.csv")
sales = spark.read.option("header", "true").csv("/samples/sales.csv")
customers = customers.dropna(subset=["customer_id"])
sales = sales.dropna(subset=["customer_id"])

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
    "city": "Unknown",
    "name": "Unknown"
})
df_clean = df_clean.dropDuplicates()
df_clean = df_clean.filter(col("age") >= 0)
print("Cleaned Data")
df_clean.show()

print("Row count after cleaning:", df_clean.count())
