"""
============================================================
File Name : test_dim_weather.py
Project   : Weather Data Engineering Pipeline
Author    : Arul G

Purpose:
    Integration test for the Weather Dimension (dim_weather) loader.
    Verifies connection to PostgreSQL, incremental LEFT ANTI JOIN,
    and row insertion into dim_weather.
============================================================
"""

from pathlib import Path
from pyspark.sql import SparkSession

from scripts.warehouse.dim_weather import load_dim_weather


def create_spark_session() -> SparkSession:
    """
    Create a local SparkSession configured with the PostgreSQL JDBC driver.
    Dynamically locates the jar file across Windows and Linux environments.
    """
    jdbc_path = (
        Path(__file__).resolve().parent.parent
        / "drivers"
        / "postgresql-42.7.13.jar"
    )

    if not jdbc_path.exists():
        fallback_jar = Path("/usr/share/java/postgresql-42.7.10.jar")
        if fallback_jar.exists():
            jdbc_jar = str(fallback_jar)
        else:
            raise FileNotFoundError(f"PostgreSQL JDBC Driver not found at: {jdbc_path}")
    else:
        jdbc_jar = str(jdbc_path.resolve())

    import os
    builder = (
        SparkSession.builder
        .appName("Test Weather Dimension")
        .master("local[*]")
        .config("spark.driver.extraClassPath", jdbc_jar)
        .config("spark.executor.extraClassPath", jdbc_jar)
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.hadoop.hadoop.native.lib", "false")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
    )
    if os.name != "nt":
        builder = builder.config("spark.jars", jdbc_jar)
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def main() -> None:
    spark = None

    try:
        spark = create_spark_session()

        weather_df = load_dim_weather(spark)

        print("\n" + "=" * 70)
        print("WEATHER DIMENSION TEST")
        print("=" * 70)

        count = weather_df.count()
        print(f"Inserted Rows : {count}")

        if count > 0:
            weather_df.show(truncate=False)
        else:
            print("No new weather records.")

        print("=" * 70)
        print("TEST COMPLETED SUCCESSFULLY")
        print("=" * 70)

    except Exception as error:
        print(f"\nTest Failed : {error}")
        raise

    finally:
        if spark:
            spark.stop()


if __name__ == "__main__":
    main()