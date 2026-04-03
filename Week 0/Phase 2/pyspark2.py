# Initialize Spark session
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('Spark Playground').getOrCreate()

# Load the customers.csv dataset
df = spark.read.format('csv').option('header', 'true').load('/samples/customers.csv')

# Show the first few rows of the DataFrame
df.show(5)

# Display the DataFrame using the display() function.
display(df)