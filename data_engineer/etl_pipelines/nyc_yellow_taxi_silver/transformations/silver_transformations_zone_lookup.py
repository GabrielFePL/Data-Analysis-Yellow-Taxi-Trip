from pyspark import pipelines as dp
import pyspark.sql.functions as f


TAXI_ZONE_LOOKUP_SOURCE = (
    "nyc_taxi.bronze."
    "bronze_taxi_zone_lookup"
)


ZONE_LOOKUP_EXPECTATIONS = {
    "location_id_present": (
        "location_id IS NOT NULL"
    ),
    "borough_present": (
        "borough IS NOT NULL "
        "AND length(trim(borough)) > 0"
    ),
    "zone_present": (
        "zone IS NOT NULL "
        "AND length(trim(zone)) > 0"
    ),
    "service_zone_present": (
        "service_zone IS NOT NULL "
        "AND length(trim(service_zone)) > 0"
    ),
}


@dp.materialized_view(
    name="silver_taxi_zone_lookup",
    comment=(
        "Dimensão padronizada de zonas da NYC Taxi "
        "and Limousine Commission."
    ),
    cluster_by_auto=True,
)
@dp.expect_all_or_fail(
    ZONE_LOOKUP_EXPECTATIONS
)
def silver_taxi_zone_lookup():
    return (
        spark.read.table(
            TAXI_ZONE_LOOKUP_SOURCE
        )
        .select(
            f.col("LocationID")
            .cast("int")
            .alias("location_id"),

            f.trim(
                f.col("Borough")
            ).alias("borough"),

            f.trim(
                f.col("Zone")
            ).alias("zone"),

            f.trim(
                f.col("service_zone")
            ).alias("service_zone"),

            f.col("_source_system"),

            f.col("_source_format"),

            f.col("_source_path"),

            f.col(
                "_pipeline_refresh_timestamp"
            ).alias(
                "_bronze_refresh_timestamp"
            ),
        )
        .withColumn(
            "_silver_refresh_timestamp",
            f.current_timestamp(),
        )
    )