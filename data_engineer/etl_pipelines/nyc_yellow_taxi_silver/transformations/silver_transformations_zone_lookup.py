from pyspark import pipelines as dp
import pyspark.sql.functions as f
from pyspark.sql.window import Window


TAXI_ZONE_LOOKUP_SOURCE = (
    "nyc_taxi.bronze."
    "bronze_taxi_zone_lookup"
)


ZONE_LOOKUP_EXPECTATIONS = {
    "location_id_present": (
        "location_id IS NOT NULL"
    ),
    "location_id_positive": (
        "location_id > 0"
    ),
    "location_id_unique": (
        "_duplicate_location_id_count = 1"
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


def _zone_quality_reasons():
    reasons = f.array(
        f.when(
            f.col("location_id").isNull(),
            f.lit("LOCATION_ID_MISSING"),
        ),
        f.when(
            f.col("location_id").isNotNull()
            & (f.col("location_id") <= f.lit(0)),
            f.lit("LOCATION_ID_NOT_POSITIVE"),
        ),
        f.when(
            f.col("_duplicate_location_id_count")
            > f.lit(1),
            f.lit("DUPLICATE_LOCATION_ID"),
        ),
        f.when(
            f.col("borough").isNull()
            | (f.length(f.trim("borough")) == f.lit(0)),
            f.lit("BOROUGH_MISSING"),
        ),
        f.when(
            f.col("zone").isNull()
            | (f.length(f.trim("zone")) == f.lit(0)),
            f.lit("ZONE_MISSING"),
        ),
        f.when(
            f.col("service_zone").isNull()
            | (
                f.length(f.trim("service_zone"))
                == f.lit(0)
            ),
            f.lit("SERVICE_ZONE_MISSING"),
        ),
    )

    return f.filter(
        reasons,
        lambda reason: reason.isNotNull(),
    )


@dp.materialized_view(
    name="silver_taxi_zone_lookup_classified",
    comment=(
        "Dataset privado de zonas padronizadas e "
        "classificadas entre Silver e quarentena."
    ),
    private=True,
)
@dp.expect_all(
    ZONE_LOOKUP_EXPECTATIONS
)
def silver_taxi_zone_lookup_classified():
    duplicate_window = Window.partitionBy(
        "location_id"
    )

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
            "_duplicate_location_id_count",
            f.count(f.lit(1)).over(
                duplicate_window
            ),
        )
        .withColumn(
            "_dq_reasons",
            _zone_quality_reasons(),
        )
        .withColumn(
            "_is_quarantined",
            f.size("_dq_reasons") > f.lit(0),
        )
        .withColumn(
            "_silver_refresh_timestamp",
            f.current_timestamp(),
        )
    )


@dp.materialized_view(
    name="silver_taxi_zone_lookup",
    comment=(
        "DimensÃ£o validada de zonas da NYC Taxi "
        "and Limousine Commission."
    ),
    cluster_by_auto=True,
)
@dp.expect_or_fail(
    "only_valid_silver_records",
    "_is_quarantined = false",
)
def silver_taxi_zone_lookup():
    return (
        spark.read.table(
            "silver_taxi_zone_lookup_classified"
        )
        .where(~f.col("_is_quarantined"))
    )


@dp.materialized_view(
    name="quarantine_taxi_zone_lookup",
    comment=(
        "Zonas rejeitadas pelo contrato de "
        "qualidade da camada Silver."
    ),
    cluster_by_auto=True,
)
@dp.expect_or_fail(
    "only_quarantined_records",
    "_is_quarantined = true",
)
def quarantine_taxi_zone_lookup():
    return (
        spark.read.table(
            "silver_taxi_zone_lookup_classified"
        )
        .where(f.col("_is_quarantined"))
    )