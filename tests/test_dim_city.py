"""
Test Module: test_dim_city.py

Purpose
-------
This script tests the City Dimension pipeline independently
before integrating it into the main ETL workflow.

Responsibilities
----------------
1. Create a Spark Session.
2. Execute the City Dimension pipeline.
3. Display the extracted data.
4. Verify the pipeline executes successfully.
"""

from pyspark.sql import SparkSession

from scripts.warehouse.dim_city import load_dim_city


def create_spark_session() -> SparkSession:
    """
    Create and configure a Spark Session for testing.

    Returns
    -------
    SparkSession
        Configured Spark session.
    """

    spark = (
        SparkSession.builder
        .appName("Test - City Dimension")
        .master("local[*]")
        .config(
            "spark.jars",
            "/usr/share/java/postgresql-42.7.10.jar"
        )
        .config(
            "spark.driver.extraClassPath",
            "/usr/share/java/postgresql-42.7.10.jar"
        )
        .getOrCreate()
    )

    return spark


def main() -> None:
    """
    Execute the City Dimension test.
    """

    spark = create_spark_session()

    try:
        city_df = load_dim_city(spark)

        print("\n")
        print("=" * 80)
        print("CITY DIMENSION TEST SUCCESSFUL")
        print("=" * 80)
        print(f"Total Unique Cities : {city_df.count()}")
        print("=" * 80)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()