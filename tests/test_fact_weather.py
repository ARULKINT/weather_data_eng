from pyspark.sql import SparkSession

from scripts.warehouse.fact_weather import load_fact_weather


def create_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("Test Fact Weather")
        .config(
            "spark.jars",
            "/usr/share/java/postgresql.jar"
        )
        .getOrCreate()
    )


def main() -> None:
    spark = None

    try:
        spark = create_spark_session()

        inserted_df = load_fact_weather(spark)

        print("\n" + "=" * 70)
        print("FACT WEATHER TEST")
        print("=" * 70)

        count = inserted_df.count()

        print(f"Inserted Rows : {count}")

        if count > 0:
            inserted_df.show(truncate=False)
        else:
            print("No new records found.")

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