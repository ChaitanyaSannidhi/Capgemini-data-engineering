from pyspark.sql import SparkSession
import urllib.request
from pyspark.sql.functions import col, when
from pyspark.sql.functions import initcap, col

# download file
url = "https://raw.githubusercontent.com/ChaitanyaSannidhi/Capgemini-data-engineering/main/DataSheets/C1.csv"
local_path = "/tmp/C1.csv"

urllib.request.urlretrieve(url, local_path)

# spark session
spark = SparkSession.builder.appName("LoadCSV").getOrCreate()

# read csv
df = spark.read.option("header","true") \
               .option("inferSchema","true") \
               .csv(local_path)

# convert blank strings to null
df = df.withColumn("Sales",
        when(col("Sales") == "blank", None).otherwise(col("Sales"))
)

df = df.withColumn("JoinDate",
        when(col("JoinDate") == "blank", None).otherwise(col("JoinDate"))
)

df = df.withColumn("Category",
        when(col("Category") == "Blank", None).otherwise(col("Category"))
)

# fill null values
df_filled = df.fillna({
    "Sales": 0,
    "JoinDate": "01-01-2023",
    "Category": "Unknown"
})


# remove row where CustomerID = 104
df_filled = df_filled.filter(col("CustomerID") != 104)
df_filled = df_filled.filter(col("CustomerID") != 108)

df_final = df_filled.replace(
    {
        "india": "India",
        "New York": "USA"
    },
    subset=["Country"]
)

df_final.show()