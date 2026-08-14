from pyspark import pipelines as dp
import pyspark.sql.functions as f


YELLOW_TRIP_SOURCE = (
    "aws_glue_nyc_taxi."
    "nyc_taxi_study."
    "yellow_tripdata"
)


@dp.materialized_view(
    name="bronze_yellow_trip_2025",
    comment=(
        "Cópia Bronze dos dados NYC Yellow Taxi 2025 "
        "provenientes do AWS Glue Data Catalog."
    ),
)
def bronze_yellow_trip_2025():
    return (
        spark.read.table(YELLOW_TRIP_SOURCE)
        .where(f.col("year") == f.lit("2025"))
        .withColumn(
            "_source_system",
            f.lit("aws_glue"),
        )
        .withColumn(
            "_source_table",
            f.lit(YELLOW_TRIP_SOURCE),
        )
        .withColumn(
            "_ingested_at",
            f.current_timestamp(),
        )
    )