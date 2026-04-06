# ETL Pipeline with Functions — Phase 4

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, count, when, round, concat_ws


# -----------------------------
# Extract
# -----------------------------
def extract_data(spark):
    customers_df = spark.read.option("header", "true").csv("/samples/customers.csv")
    sales_df = spark.read.option("header", "true").csv("/samples/sales.csv")
    return customers_df, sales_df


# -----------------------------
# Transform — Cleaning
# -----------------------------
def clean_data(customers_df, sales_df):
    # Remove null keys and duplicates
    customers_df = customers_df.dropna(subset=["customer_id"]).dropDuplicates()
    sales_df = sales_df.dropna(subset=["customer_id"]).dropDuplicates()

    # Cast data types
    customers_df = customers_df.withColumn("customer_id", col("customer_id").cast("int"))

    sales_df = sales_df.withColumn("customer_id", col("customer_id").cast("int")) \
                       .withColumn("total_amount", col("total_amount").cast("double"))

    # Filter invalid values
    sales_df = sales_df.filter(col("total_amount") > 0)

    # Create full customer name
    customers_df = customers_df.withColumn(
        "customer_name",
        concat_ws(" ", col("first_name"), col("last_name"))
    )

    return customers_df, sales_df


# -----------------------------
# Transform — Business Logic
# -----------------------------
def transform_data(customers_df, sales_df):

    # Join datasets
    df = customers_df.join(sales_df, on="customer_id", how="inner")

    # -----------------------------
    # Task 1: Daily Sales → date, total_sales
    # -----------------------------
    daily_sales_df = sales_df.groupBy("sale_date") \
        .agg(round(sum("total_amount"), 2).alias("total_sales"))

    # -----------------------------
    # Task 2: City-wise Revenue → city, total_revenue
    # -----------------------------
    city_revenue_df = df.groupBy("city") \
        .agg(round(sum("total_amount"), 2).alias("total_revenue"))

    # -----------------------------
    # Task 3: Top 5 Customers → customer_name, total_spend
    # -----------------------------
    customer_spend_df = df.groupBy("customer_id", "customer_name") \
        .agg(round(sum("total_amount"), 2).alias("total_spend"))

    top_customers_df = customer_spend_df.orderBy(col("total_spend").desc()).limit(5)

    # -----------------------------
    # Task 4: Repeat Customers (>1 order) → customer_id, order_count
    # -----------------------------
    repeat_df = sales_df.groupBy("customer_id") \
        .agg(count("*").alias("order_count")) \
        .filter(col("order_count") > 1)

    # -----------------------------
    # Task 5: Customer Segmentation
    # total_spend > 10000 → Gold
    # 5000–10000 → Silver
    # <5000 → Bronze
    # -----------------------------
    segmented_df = customer_spend_df.withColumn(
        "segment",
        when(col("total_spend") > 100, "Gold")
        .when((col("total_spend") >= 50) & (col("total_spend") <= 100), "Silver")
        .otherwise("Bronze")
    )

    # -----------------------------
    # Task 6: Final Reporting Table
    # customer_name, city, total_spend, order_count, segment
    # -----------------------------
    final_df = segmented_df.join(repeat_df, on="customer_id", how="left") \
        .join(customers_df.select("customer_id", "city"), on="customer_id", how="left") \
        .select("customer_name", "city", "total_spend", "order_count", "segment")

    return final_df


# -----------------------------
# Load
# -----------------------------
def load_data(final_df):
    # Task 7: Save Output
    final_df.write.mode("overwrite").csv("/samples/output/report")


# -----------------------------
# Main
# -----------------------------
def main():
    spark = SparkSession.builder.appName("ETL_Functions").getOrCreate()

    customers_df, sales_df = extract_data(spark)
    customers_df, sales_df = clean_data(customers_df, sales_df)
    final_df = transform_data(customers_df, sales_df)

    load_data(final_df)

    final_df.show()


if __name__ == "__main__":
    main()