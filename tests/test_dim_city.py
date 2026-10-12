"""
============================================================
File Name : test_dim_city.py
Project   : Weather Data Engineering Pipeline
Author    : Arul G

Purpose:
    Integration test for the City Dimension (dim_city) loader.
    Verifies connection to PostgreSQL, incremental LEFT ANTI JOIN,
    and row insertion into dim_city.
============================================================
"""

from pathlib import Path
from pyspark.sql import SparkSession

from scripts.warehouse.dim_city import load_dim_city


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
        .appName("Test City Dimension")
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

        city_df = load_dim_city(spark)

        print("\n" + "=" * 70)
        print("CITY DIMENSION TEST")
        print("=" * 70)

        count = city_df.count()
        print(f"Inserted Rows : {count}")

        if count > 0:
            city_df.show(truncate=False)
        else:
            print("No new city records.")

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