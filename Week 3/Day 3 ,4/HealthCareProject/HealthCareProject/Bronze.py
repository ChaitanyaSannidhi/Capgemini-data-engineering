# Databricks notebook source

# COMMAND ----------

dbutils.fs.ls("abfss://healthcareproject@healthcareproject12.dfs.core.windows.net/")

# COMMAND ----------

df = spark.read.option("header", True).csv(
  "abfss://healthcareproject@healthcareproject12.dfs.core.windows.net/healthcare-raw"
)

display(df)

# COMMAND ----------

df = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("abfss://healthcareproject@healthcareproject12.dfs.core.windows.net/healthcare-raw")

df.display()

# COMMAND ----------

df.write.format("delta") \
    .mode("overwrite") \
    .save("abfss://healthcareproject@healthcareproject12.dfs.core.windows.net/bronze/healthcare_bronze")

# COMMAND ----------

df = spark.read.format("delta").load(
  "abfss://healthcareproject@healthcareproject12.dfs.core.windows.net/bronze/healthcare_bronze"
)

df.printSchema()
df.count()
df.select("No-show").groupBy("No-show").count().show()

# COMMAND ----------

