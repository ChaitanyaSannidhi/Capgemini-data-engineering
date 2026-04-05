from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.window import Window

# ------------------------------------------------------------
# INIT SPARK
# ------------------------------------------------------------
spark = SparkSession.builder.appName("Final_ETL_Pipeline").getOrCreate()


# ------------------------------------------------------------
# STEP 1: EXTRACT
# ------------------------------------------------------------
def extract():
    customers = spark.read.option("header","true").option("inferSchema","true").csv("/samples/customers.csv")
    sales = spark.read.option("header","true").option("inferSchema","true").csv("/samples/sales.csv")
    return customers, sales


# ------------------------------------------------------------
# STEP 2: CLEAN
# ------------------------------------------------------------
def clean(customers, sales):
    customers = customers.dropna(subset=["customer_id"])
    sales = sales.dropna(subset=["customer_id","total_amount"])

    customers = customers.withColumn("customer_id", F.col("customer_id").cast("int"))
    sales = sales.withColumn("customer_id", F.col("customer_id").cast("int"))
    sales = sales.withColumn("total_amount", F.col("total_amount").cast("double"))

    return customers, sales


# ------------------------------------------------------------
# STEP 3: TRANSFORM
# ------------------------------------------------------------
def transform(customers, sales):
    return customers.join(sales, "customer_id")


# ------------------------------------------------------------
# STEP 4: METRICS
# ------------------------------------------------------------
def build_metrics(df, sales):

    # 1. Daily Sales
    daily_sales = sales.groupBy("sale_date") \
        .agg(F.round(F.sum("total_amount"),2).alias("daily_sales"))

    # 2. City-wise Revenue
    city_revenue = df.groupBy("city") \
        .agg(F.round(F.sum("total_amount"),2).alias("city_revenue"))

    # 3. Repeat Customers (>2 orders)
    repeat_customers = sales.groupBy("customer_id") \
        .agg(F.count("*").alias("order_count")) \
        .filter(F.col("order_count") > 2)

    # 4. Highest spending customer in each city
    spend = df.groupBy("city","customer_id") \
        .agg(F.sum("total_amount").alias("total_spent"))

    window = Window.partitionBy("city").orderBy(F.col("total_spent").desc())

    top_customers = spend.withColumn("rank", F.rank().over(window)) \
        .filter(F.col("rank") == 1)

    # 5. Final Reporting Table
    final_report = df.groupBy("customer_id","city") \
        .agg(
            F.round(F.sum("total_amount"),2).alias("total_spent"),
            F.count("*").alias("order_count")
        )

    return daily_sales, city_revenue, repeat_customers, top_customers, final_report


# ------------------------------------------------------------
# STEP 5: LOAD
# ------------------------------------------------------------
def load(daily, city, repeat, top, final):
    print("\n=== Daily Sales ===")
    daily.show()

    print("\n=== City Revenue ===")
    city.show()

    print("\n=== Repeat Customers ===")
    repeat.show()

    print("\n=== Top Customers per City ===")
    top.show()

    print("\n=== Final Report ===")
    final.show()


# ------------------------------------------------------------
# MAIN PIPELINE FUNCTION
# ------------------------------------------------------------
def run_pipeline():
    customers, sales = extract()
    customers, sales = clean(customers, sales)
    df = transform(customers, sales)

    daily, city, repeat, top, final = build_metrics(df, sales)

    load(daily, city, repeat, top, final)


# ------------------------------------------------------------
# EXECUTE PIPELINE
# ------------------------------------------------------------
run_pipeline()