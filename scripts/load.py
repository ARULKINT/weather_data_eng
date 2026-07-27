from pyspark.sql import SparkSession
from pyspark.sql.functions import col

from config.settings import (
    DB_HOST,
    DB_PORT,
    DB_NAME,
    DB_USER,
    DB_PASSWORD
)

# Create Spark Session
spark = SparkSession.builder \
    .appName("Weather Data Load") \
    .config(
        "spark.jars",
        "/usr/share/java/postgresql-42.7.10.jar"
    ) \
    .getOrCreate()

# Read transformed JSON
df = spark.read.option("multiLine", "true").json("data/raw/*.json")

# Transform
weather_df = df.select(
    col("name").alias("city"),
    col("coord.lat").alias("latitude"),
    col("coord.lon").alias("longitude"),
    col("main.temp").alias("temperature"),
    col("main.humidity").alias("humidity"),
    col("main.pressure").alias("pressure"),
    col("wind.speed").alias("wind_speed"),
    col("weather")[0]["main"].alias("weather"),
    col("weather")[0]["description"].alias("description")
)

# JDBC URL
jdbc_url = f"jdbc:postgresql://{DB_HOST}:{DB_PORT}/{DB_NAME}"

# Write to PostgreSQL
weather_df.write \
    .format("jdbc") \
    .option("url", jdbc_url) \
    .option("dbtable", "weather_data") \
    .option("user", DB_USER) \
    .option("password", DB_PASSWORD) \
    .option("driver", "org.postgresql.Driver") \
    .mode("append") \
    .save()

print("✅ Data loaded successfully!")

spark.stop()