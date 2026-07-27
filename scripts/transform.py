from pyspark.sql import SparkSession

# Create Spark Session
spark = SparkSession.builder \
    .appName("Weather Data Transformation") \
    .getOrCreate()

# Read JSON file
df = spark.read.option("multiLine", "true").json("data/raw/*.json")

# Print schema
print("===== SCHEMA =====")
df.printSchema()

# Show data
print("===== DATA =====")
df.show(truncate=False)

# Stop Spark
spark.stop()