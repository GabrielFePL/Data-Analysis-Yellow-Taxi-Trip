from pyspark import pipelines as dp
import pyspark.sql.functions as f
from pyspark.sql.types import (
    IntegerType,
    StringType,
    StructField,
    StructType,
)

TAXI_ZONE_LOOKUP_PATH = (
    "/Volumes/nyc_taxi/landing/"
    "taxi_zone_lookup/"
    "taxi_zone_lookup.csv"
)

TAXI_ZONE_LOOKUP_SCHEMA = StructType([
    StructField(
        "LocationID",
        IntegerType(),
        True,
    ),
    StructField(
        "Borough",
        StringType(),
        True,
    ),
    StructField(
        "Zone",
        StringType(),
        True,
    ),
    StructField(
        "service_zone",
        StringType(),
        True,
    ),
])


@dp.materialized_view(
    name="bronze_taxi_zone_lookup",
    comment=(
        "Snapshot Bronze do NYC Taxi Zone Lookup "
        "carregado de um Unity Catalog Volume."
    ),
)
def bronze_taxi_zone_lookup():
    return (
        spark.read
        .format("csv")
        .schema(TAXI_ZONE_LOOKUP_SCHEMA)
        .option("header", "true")
        .option("mode", "FAILFAST")
        .option("encoding", "UTF-8")
        .option("enforceSchema", "false")
        .load(TAXI_ZONE_LOOKUP_PATH)
        .withColumn(
            "_source_system",
            f.lit("unity_catalog_volume"),
        )
        .withColumn(
            "_source_format",
            f.lit("csv"),
        )
        .withColumn(
            "_source_path",
            f.lit(TAXI_ZONE_LOOKUP_PATH),
        )
        .withColumn(
            "_pipeline_refresh_timestamp",
            f.current_timestamp(),
        )
    )