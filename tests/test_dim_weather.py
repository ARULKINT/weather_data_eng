from pyspark.sql import SparkSession

from scripts.warehouse.dim_weather import load_dim_weather


def create_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("Test Weather Dimension")
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

        weather_df = load_dim_weather(spark)

        print("\n" + "=" * 70)
        print("WEATHER DIMENSION TEST")
        print("=" * 70)

        print(f"Inserted Rows : {weather_df.count()}")

        if weather_df.count() > 0:
            weather_df.show(truncate=False)
        else:
            print("No new weather records.")

        print("=" * 70)
        print("TEST COMPLETED SUCCESSFULLY")
        print("=" * 70)

    except Exception as error:
        print(error)
        raise

    finally:
        if spark:
            spark.stop()


if __name__ == "__main__":
    main()