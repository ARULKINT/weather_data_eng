from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col

from scripts.logger import logger
from scripts.warehouse.database import read_table, write_table


def load_fact_weather(spark: SparkSession) -> DataFrame:
    try:
        logger.info("=" * 80)
        logger.info("Starting Fact Weather Load")
        logger.info("=" * 80)

        weather_df = read_table(
            spark,
            "weather_data"
        )

        city_df = (
            read_table(
                spark,
                "dim_city"
            )
            .select(
                "city_id",
                "city_name"
            )
        )

        weather_dim_df = (
            read_table(
                spark,
                "dim_weather"
            )
            .select(
                "weather_id",
                "weather",
                "description"
            )
        )

        fact_df = (
            weather_df
            .join(
                city_df,
                weather_df["city"] == city_df["city_name"],
                "inner"
            )
            .join(
                weather_dim_df,
                (
                    (weather_df["weather"] == weather_dim_df["weather"])
                    &
                    (
                        weather_df["description"]
                        ==
                        weather_dim_df["description"]
                    )
                ),
                "inner"
            )
            .select(
                city_df["city_id"].alias("city_id"),
                weather_dim_df["weather_id"].alias("weather_id"),
                weather_df["temperature"].alias("temperature"),
                weather_df["humidity"].alias("humidity"),
                weather_df["pressure"].alias("pressure"),
                weather_df["wind_speed"].alias("wind_speed"),
                weather_df["collected_at"].alias("collected_at")
            )
        )

        existing_fact_df = read_table(
            spark,
            "fact_weather"
        )

        new_fact_df = (
            fact_df.alias("new")
            .join(
                existing_fact_df.alias("old"),
                (
                    (col("new.city_id") == col("old.city_id"))
                    &
                    (col("new.weather_id") == col("old.weather_id"))
                    &
                    (
                        col("new.collected_at")
                        ==
                        col("old.collected_at")
                    )
                ),
                "left_anti"
            )
        )

        new_fact_df = new_fact_df.cache()

        inserted_rows = new_fact_df.count()

        logger.info(
            "New Fact Records : %d",
            inserted_rows
        )

        if inserted_rows == 0:
            logger.info("No new fact records found.")
            return new_fact_df

        write_table(
            dataframe=new_fact_df,
            table_name="fact_weather"
        )

        logger.info(
            "Successfully inserted %d fact records.",
            inserted_rows
        )

        return new_fact_df

    except Exception as error:
        logger.exception(
            "Fact Weather loading failed."
        )
        raise   