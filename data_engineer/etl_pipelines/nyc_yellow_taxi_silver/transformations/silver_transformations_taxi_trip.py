from pyspark import pipelines as dp
import pyspark.sql.functions as f


TAXI_TRIP_SOURCE = (
    "nyc_taxi.bronze."
    "bronze_yellow_trip_2025"
)


CRITICAL_EXPECTATIONS = {
    "pickup_timestamp_present": (
        "pickup_datetime IS NOT NULL"
    ),
    "dropoff_timestamp_present": (
        "dropoff_datetime IS NOT NULL"
    ),
    "valid_trip_period": (
        "dropoff_datetime > pickup_datetime"
    ),
    "pickup_in_expected_year": (
        "year(pickup_datetime) = 2025"
    ),
    "source_partition_matches_pickup": (
        "source_year = 2025 "
        "AND source_month = month(pickup_datetime)"
    ),
    "trip_distance_present": (
        "trip_distance IS NOT NULL"
    ),
    "trip_distance_non_negative": (
        "trip_distance >= 0"
    ),
    "total_amount_present": (
        "total_amount IS NOT NULL"
    ),
    "pickup_location_present": (
        "pickup_location_id IS NOT NULL"
    ),
    "dropoff_location_present": (
        "dropoff_location_id IS NOT NULL"
    ),
    "pickup_location_resolved": (
        "pickup_zone_location_id IS NOT NULL"
    ),
    "dropoff_location_resolved": (
        "dropoff_zone_location_id IS NOT NULL"
    ),
}


HASH_COLUMNS = [
    "vendor_id",
    "pickup_datetime",
    "dropoff_datetime",
    "passenger_count",
    "trip_distance",
    "rate_code_id",
    "pickup_location_id",
    "dropoff_location_id",
    "payment_type",
    "fare_amount",
    "tip_amount",
    "total_amount",
]


def _record_hash():
    values = [
        f.coalesce(
            f.col(column_name).cast("string"),
            f.lit("<NULL>"),
        )
        for column_name in HASH_COLUMNS
    ]

    return f.sha2(
        f.concat_ws("||", *values),
        256,
    )


def _quality_reasons():
    reasons = f.array(
        f.when(
            f.col("pickup_datetime").isNull(),
            f.lit("PICKUP_TIMESTAMP_MISSING"),
        ),
        f.when(
            f.col("dropoff_datetime").isNull(),
            f.lit("DROPOFF_TIMESTAMP_MISSING"),
        ),
        f.when(
            f.col("pickup_datetime").isNotNull()
            & f.col("dropoff_datetime").isNotNull()
            & (
                f.col("dropoff_datetime")
                <= f.col("pickup_datetime")
            ),
            f.lit("INVALID_TRIP_PERIOD"),
        ),
        f.when(
            f.col("pickup_datetime").isNotNull()
            & (
                f.year("pickup_datetime")
                != f.lit(2025)
            ),
            f.lit("PICKUP_OUTSIDE_2025"),
        ),
        f.when(
            f.col("pickup_datetime").isNotNull()
            & (
                (f.col("source_year") != f.lit(2025))
                | (
                    f.col("source_month")
                    != f.month("pickup_datetime")
                )
            ),
            f.lit("SOURCE_PARTITION_MISMATCH"),
        ),
        f.when(
            f.col("trip_distance").isNull(),
            f.lit("TRIP_DISTANCE_MISSING"),
        ),
        f.when(
            f.col("trip_distance") < f.lit(0),
            f.lit("NEGATIVE_TRIP_DISTANCE"),
        ),
        f.when(
            f.col("total_amount").isNull(),
            f.lit("TOTAL_AMOUNT_MISSING"),
        ),
        f.when(
            f.col("pickup_location_id").isNull(),
            f.lit("PICKUP_LOCATION_MISSING"),
        ),
        f.when(
            f.col("dropoff_location_id").isNull(),
            f.lit("DROPOFF_LOCATION_MISSING"),
        ),
        f.when(
            f.col("pickup_location_id").isNotNull()
            & f.col(
                "pickup_zone_location_id"
            ).isNull(),
            f.lit("PICKUP_LOCATION_NOT_RESOLVED"),
        ),
        f.when(
            f.col("dropoff_location_id").isNotNull()
            & f.col(
                "dropoff_zone_location_id"
            ).isNull(),
            f.lit("DROPOFF_LOCATION_NOT_RESOLVED"),
        ),
    )

    return f.filter(
        reasons,
        lambda reason: reason.isNotNull(),
    )


@dp.materialized_view(
    name="silver_yellow_trip_classified",
    comment=(
        "Dataset privado de viagens padronizadas "
        "e classificadas entre Silver e quarentena."
    ),
    private=True,
)
@dp.expect_all(CRITICAL_EXPECTATIONS)
def silver_yellow_trip_classified():
    trips_df = (
        spark.read.table(TAXI_TRIP_SOURCE)
        .select(
            f.col("VendorID")
            .cast("int")
            .alias("vendor_id"),

            f.col("tpep_pickup_datetime")
            .cast("timestamp")
            .alias("pickup_datetime"),

            f.col("tpep_dropoff_datetime")
            .cast("timestamp")
            .alias("dropoff_datetime"),

            f.col("passenger_count")
            .cast("int")
            .alias("passenger_count"),

            f.col("trip_distance")
            .cast("decimal(18, 3)")
            .alias("trip_distance"),

            f.col("RatecodeID")
            .cast("int")
            .alias("rate_code_id"),

            f.upper(
                f.trim(
                    f.col("store_and_fwd_flag")
                )
            ).alias("store_and_forward_flag"),

            f.col("PULocationID")
            .cast("int")
            .alias("pickup_location_id"),

            f.col("DOLocationID")
            .cast("int")
            .alias("dropoff_location_id"),

            f.col("payment_type")
            .cast("int")
            .alias("payment_type"),

            f.col("fare_amount")
            .cast("decimal(18, 2)")
            .alias("fare_amount"),

            f.col("extra")
            .cast("decimal(18, 2)")
            .alias("extra_amount"),

            f.col("mta_tax")
            .cast("decimal(18, 2)")
            .alias("mta_tax_amount"),

            f.col("tip_amount")
            .cast("decimal(18, 2)")
            .alias("tip_amount"),

            f.col("tolls_amount")
            .cast("decimal(18, 2)")
            .alias("tolls_amount"),

            f.col("improvement_surcharge")
            .cast("decimal(18, 2)")
            .alias(
                "improvement_surcharge_amount"
            ),

            f.col("total_amount")
            .cast("decimal(18, 2)")
            .alias("total_amount"),

            f.col("congestion_surcharge")
            .cast("decimal(18, 2)")
            .alias(
                "congestion_surcharge_amount"
            ),

            f.col("Airport_fee")
            .cast("decimal(18, 2)")
            .alias("airport_fee_amount"),

            f.col("cbd_congestion_fee")
            .cast("decimal(18, 2)")
            .alias(
                "cbd_congestion_fee_amount"
            ),

            f.col("year")
            .cast("int")
            .alias("source_year"),

            f.col("month")
            .cast("int")
            .alias("source_month"),
        )
    )

    zones_df = (
        spark.read.table(
            "silver_taxi_zone_lookup"
        )
        .select(
            "location_id",
            "borough",
            "zone",
            "service_zone",
        )
    )

    pickup_zones_df = (
        zones_df
        .select(
            f.col("location_id").alias(
                "pickup_zone_location_id"
            ),
            f.col("borough").alias(
                "pickup_borough"
            ),
            f.col("zone").alias(
                "pickup_zone"
            ),
            f.col("service_zone").alias(
                "pickup_service_zone"
            ),
        )
    )

    dropoff_zones_df = (
        zones_df
        .select(
            f.col("location_id").alias(
                "dropoff_zone_location_id"
            ),
            f.col("borough").alias(
                "dropoff_borough"
            ),
            f.col("zone").alias(
                "dropoff_zone"
            ),
            f.col("service_zone").alias(
                "dropoff_service_zone"
            ),
        )
    )

    return (
        trips_df
        .join(
            pickup_zones_df,
            (
                f.col("pickup_location_id")
                == f.col(
                    "pickup_zone_location_id"
                )
            ),
            "left",
        )
        .join(
            dropoff_zones_df,
            (
                f.col("dropoff_location_id")
                == f.col(
                    "dropoff_zone_location_id"
                )
            ),
            "left",
        )
        .withColumn(
            "pickup_date",
            f.to_date("pickup_datetime"),
        )
        .withColumn(
            "pickup_year",
            f.year("pickup_datetime"),
        )
        .withColumn(
            "pickup_month",
            f.month("pickup_datetime"),
        )
        .withColumn(
            "pickup_day",
            f.dayofmonth("pickup_datetime"),
        )
        .withColumn(
            "pickup_hour",
            f.hour("pickup_datetime"),
        )
        .withColumn(
            "pickup_day_of_week",
            f.dayofweek("pickup_datetime"),
        )
        .withColumn(
            "trip_duration_minutes",
            f.round(
                (
                    f.col(
                        "dropoff_datetime"
                    ).cast("long")
                    - f.col(
                        "pickup_datetime"
                    ).cast("long")
                )
                / f.lit(60),
                2,
            ),
        )
        .withColumn(
            "_record_hash",
            _record_hash(),
        )
        .withColumn(
            "_dq_reasons",
            _quality_reasons(),
        )
        .withColumn(
            "_is_quarantined",
            (
                f.size(
                    f.col("_dq_reasons")
                )
                > f.lit(0)
            ),
        )
        .withColumn(
            "_is_zero_distance",
            f.coalesce(
                f.col("trip_distance") == f.lit(0),
                f.lit(False),
            ),
        )
        .withColumn(
            "_is_passenger_count_missing",
            f.col("passenger_count").isNull(),
        )
        .withColumn(
            "_is_non_positive_passenger_count",
            f.coalesce(
                (
                    f.col("passenger_count")
                    <= f.lit(0)
                ),
                f.lit(False),
            ),
        )
        .withColumn(
            "_is_negative_total_amount",
            f.coalesce(
                f.col("total_amount") < f.lit(0),
                f.lit(False),
            ),
        )
        .withColumn(
            "_is_long_trip",
            f.coalesce(
                (
                    f.col(
                        "trip_duration_minutes"
                    )
                    > f.lit(360)
                ),
                f.lit(False),
            ),
        )
        .withColumn(
            "_silver_refresh_timestamp",
            f.current_timestamp(),
        )
    )


@dp.materialized_view(
    name="silver_yellow_trip_2025",
    comment=(
        "Viagens NYC Yellow Taxi de 2025 "
        "padronizadas e enriquecidas com zonas."
    ),
    cluster_by_auto=True,
)
@dp.expect_or_fail(
    "only_valid_silver_records",
    "_is_quarantined = false",
)
def silver_yellow_trip_2025():
    return (
        spark.read.table(
            "silver_yellow_trip_classified"
        )
        .where(
            ~f.col("_is_quarantined")
        )
    )


@dp.materialized_view(
    name="quarantine_yellow_trip_2025",
    comment=(
        "Viagens rejeitadas pelo contrato "
        "de qualidade da camada Silver."
    ),
    cluster_by_auto=True,
)
@dp.expect_or_fail(
    "only_quarantined_records",
    "_is_quarantined = true",
)
def quarantine_yellow_trip_2025():
    return (
        spark.read.table(
            "silver_yellow_trip_classified"
        )
        .where(
            f.col("_is_quarantined")
        )
    )