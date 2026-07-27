from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("Weather Data Engineering")
    .master("local[*]")
    .getOrCreate()
)

print("Spark Version:", spark.version)

spark.stop()

