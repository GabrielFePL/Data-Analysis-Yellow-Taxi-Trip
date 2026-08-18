from pyspark import pipelines as dp
import pyspark.sql.functions as f
from pyspark.sql.window import Window


TAXI_TRIP_SOURCE = spark.conf.get(
    "nyc_taxi.silver.trip_source_table",
    "nyc_taxi.bronze.bronze_yellow_trip_2025",
)

TAXI_ZONE_LOOKUP_SOURCE = spark.conf.get(
    "nyc_taxi.silver.zone_lookup_table",
    "silver_taxi_zone_lookup",
)

EXPECTED_PICKUP_YEAR = int(
    spark.conf.get(
        "nyc_taxi.silver.expected_pickup_year",
        "2025",
    )
)

MAX_VALID_TRIP_DURATION_MINUTES = int(
    spark.conf.get(
        "nyc_taxi.silver.max_trip_duration_minutes",
        "360",
    )
)

FINANCIAL_RECONCILIATION_TOLERANCE = (
    spark.conf.get(
        "nyc_taxi.silver.financial_tolerance",
        "0.01",
    )
)

QUALITY_RULE_VERSION = "2.0"

VALID_PAYMENT_TYPE_CODES = (0, 1, 2, 3, 4, 5, 6)
VALID_RATE_CODE_IDS = (1, 2, 3, 4, 5, 6, 99)
VALID_VENDOR_IDS = (1, 2, 6, 7)


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
    "trip_duration_within_limit": (
        "trip_duration_minutes <= "
        f"{MAX_VALID_TRIP_DURATION_MINUTES}"
    ),
    "pickup_in_expected_year": (
        "year(pickup_datetime) = "
        f"{EXPECTED_PICKUP_YEAR}"
    ),
    "source_partition_matches_pickup": (
        "source_year IS NOT NULL "
        "AND source_month IS NOT NULL "
        f"AND source_year = {EXPECTED_PICKUP_YEAR} "
        "AND source_month = month(pickup_datetime)"
    ),
    "trip_distance_present": (
        "trip_distance IS NOT NULL"
    ),
    "trip_distance_non_negative": (
        "trip_distance >= 0"
    ),
    "passenger_count_non_negative_when_reported": (
        "passenger_count IS NULL "
        "OR passenger_count >= 0"
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
    "record_not_duplicated": (
        "_is_duplicate_record = false"
    ),
}


PUBLISHED_SILVER_EXPECTATIONS = {
    "only_valid_silver_records": (
        "_is_quarantined = false"
    ),
    "critical_reason_array_empty": (
        "size(_dq_reasons) = 0"
    ),
    "record_hash_present": (
        "record_hash IS NOT NULL"
    ),
    "trip_volume_eligible": (
        "_is_trip_volume_metric_eligible = true"
    ),
    "recorded_amount_eligible": (
        "_is_recorded_amount_metric_eligible = true"
    ),
    "route_eligible": (
        "_is_route_metric_eligible = true"
    ),
    "efficiency_eligibility_consistent": (
        "_is_efficiency_metric_eligible = "
        "(_is_distance_metric_eligible AND "
        "_is_duration_metric_eligible)"
    ),
    "efficiency_matches_quality_contract": (
        "_is_efficiency_metric_eligible = "
        "_is_efficiency_kpi_eligible"
    ),
    "ml_standard_trip_consistent": (
        "_is_ml_standard_trip_eligible = ("
        "_is_standard_operational_trip_eligible "
        "AND _is_passenger_metric_eligible "
        "AND _is_ml_financial_feature_eligible "
        "AND _is_ml_categorical_feature_eligible)"
    ),
    "eligibility_status_valid": (
        "_eligibility_status IN ("
        "'FULLY_ELIGIBLE', "
        "'PARTIALLY_ELIGIBLE', "
        "'REVIEW_REQUIRED')"
    ),
    "eligibility_reason_count_consistent": (
        "_eligibility_restriction_count = "
        "size(_eligibility_reasons)"
    ),
    "fully_eligible_has_no_restrictions": (
        "_eligibility_status <> 'FULLY_ELIGIBLE' "
        "OR _eligibility_restriction_count = 0"
    ),
    "review_status_consistent": (
        "_requires_data_review = "
        "(_eligibility_status = 'REVIEW_REQUIRED')"
    ),
}


QUARANTINE_EXPECTATIONS = {
    "only_quarantined_records": (
        "_is_quarantined = true"
    ),
    "critical_reason_present": (
        "size(_dq_reasons) > 0"
    ),
}


HASH_COLUMNS = (
    "vendor_id",
    "pickup_datetime",
    "dropoff_datetime",
    "passenger_count",
    "trip_distance",
    "rate_code_id",
    "store_and_fwd_flag",
    "pickup_location_id",
    "dropoff_location_id",
    "payment_type",
    "fare_amount",
    "extra_amount",
    "mta_tax_amount",
    "tip_amount",
    "tolls_amount",
    "improvement_surcharge_amount",
    "total_amount",
    "congestion_surcharge_amount",
    "airport_fee_amount",
    "cbd_congestion_fee_amount",
    "source_year",
    "source_month",
)


DUPLICATE_WARNING = (
    "DUPLICATE_GROUP_CANONICAL_RECORD"
)

PASSENGER_WARNINGS = (
    "FLEX_FARE_PASSENGER_COUNT_MISSING",
    "PASSENGER_COUNT_MISSING",
    "NON_POSITIVE_PASSENGER_COUNT",
)

DISTANCE_WARNINGS = (
    "ZERO_TRIP_DISTANCE",
)

FINANCIAL_RECONCILIATION_WARNINGS = (
    "FINANCIAL_RECONCILIATION_GAP",
)

NEGATIVE_FINANCIAL_WARNINGS = (
    "NEGATIVE_TOTAL_AMOUNT",
    "NEGATIVE_FARE_AMOUNT",
    "NEGATIVE_TIP_AMOUNT",
    "NEGATIVE_FINANCIAL_COMPONENT",
)

TIP_WARNINGS = (
    "TIP_REPORTED_OUTSIDE_SUPPORTED_PAYMENT",
)

RATE_CODE_WARNINGS = (
    "FLEX_FARE_RATE_CODE_MISSING",
    "RATE_CODE_MISSING_OR_UNMAPPED",
)

VENDOR_WARNINGS = (
    "VENDOR_MISSING_OR_UNMAPPED",
)

PAYMENT_TYPE_WARNINGS = (
    "PAYMENT_TYPE_MISSING_OR_UNMAPPED",
)

CROSS_YEAR_WARNINGS = (
    "CROSS_YEAR_TRIP",
)

REVIEW_WARNINGS = (
    "FINANCIAL_RECONCILIATION_GAP",
    "PASSENGER_COUNT_MISSING",
    "NON_POSITIVE_PASSENGER_COUNT",
    "RATE_CODE_MISSING_OR_UNMAPPED",
    "VENDOR_MISSING_OR_UNMAPPED",
    "PAYMENT_TYPE_MISSING_OR_UNMAPPED",
    "TIP_REPORTED_OUTSIDE_SUPPORTED_PAYMENT",
    DUPLICATE_WARNING,
)


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


def _financial_component_amount():
    component_columns = (
        "fare_amount",
        "extra_amount",
        "mta_tax_amount",
        "tip_amount",
        "tolls_amount",
        "improvement_surcharge_amount",
        "congestion_surcharge_amount",
        "airport_fee_amount",
        "cbd_congestion_fee_amount",
    )

    amount = f.lit(0).cast("decimal(20, 2)")
    for column_name in component_columns:
        amount = amount + f.coalesce(
            f.col(column_name),
            f.lit(0).cast("decimal(20, 2)"),
        )

    return amount.cast("decimal(20, 2)")


def _critical_quality_reasons():
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
                != f.lit(EXPECTED_PICKUP_YEAR)
            ),
            f.lit("PICKUP_OUTSIDE_EXPECTED_YEAR"),
        ),
        f.when(
            f.col("pickup_datetime").isNotNull()
            & (
                f.col("source_year").isNull()
                | f.col("source_month").isNull()
                | (
                    f.col("source_year")
                    != f.lit(EXPECTED_PICKUP_YEAR)
                )
                | (
                    f.col("source_month")
                    != f.month("pickup_datetime")
                )
            ),
            f.lit("SOURCE_PARTITION_MISMATCH"),
        ),
        f.when(
            f.col("trip_duration_minutes").isNotNull()
            & (
                f.col("trip_duration_minutes")
                > f.lit(
                    MAX_VALID_TRIP_DURATION_MINUTES
                )
            ),
            f.lit("TRIP_DURATION_EXCEEDS_LIMIT"),
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
            f.col("passenger_count") < f.lit(0),
            f.lit("NEGATIVE_PASSENGER_COUNT"),
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
        f.when(
            f.col("_is_duplicate_record"),
            f.lit("DUPLICATE_TRIP_RECORD"),
        ),
    )

    return f.filter(
        reasons,
        lambda reason: reason.isNotNull(),
    )


def _warning_reasons():
    reasons = f.array(
        f.when(
            f.col("_is_duplicate_group")
            & (~f.col("_is_duplicate_record")),
            f.lit(DUPLICATE_WARNING),
        ),
        f.when(
            f.col("_is_zero_distance"),
            f.lit("ZERO_TRIP_DISTANCE"),
        ),
        f.when(
            f.col(
                "_is_expected_flex_fare_passenger_gap"
            ),
            f.lit(
                "FLEX_FARE_PASSENGER_COUNT_MISSING"
            ),
        ),
        f.when(
            f.col(
                "_is_unexpected_passenger_count_missing"
            ),
            f.lit("PASSENGER_COUNT_MISSING"),
        ),
        f.when(
            f.col("_is_non_positive_passenger_count"),
            f.lit("NON_POSITIVE_PASSENGER_COUNT"),
        ),
        f.when(
            f.col("_is_negative_total_amount"),
            f.lit("NEGATIVE_TOTAL_AMOUNT"),
        ),
        f.when(
            f.col("_is_negative_fare_amount"),
            f.lit("NEGATIVE_FARE_AMOUNT"),
        ),
        f.when(
            f.col("_is_negative_tip_amount"),
            f.lit("NEGATIVE_TIP_AMOUNT"),
        ),
        f.when(
            f.col(
                "_has_negative_financial_component"
            ),
            f.lit("NEGATIVE_FINANCIAL_COMPONENT"),
        ),
        f.when(
            f.col("_is_financially_unreconciled"),
            f.lit("FINANCIAL_RECONCILIATION_GAP"),
        ),
        f.when(
            f.col("_is_expected_flex_fare_rate_gap"),
            f.lit("FLEX_FARE_RATE_CODE_MISSING"),
        ),
        f.when(
            f.col("_is_unexpected_rate_code_gap"),
            f.lit("RATE_CODE_MISSING_OR_UNMAPPED"),
        ),
        f.when(
            f.col("_is_vendor_missing_or_unmapped"),
            f.lit("VENDOR_MISSING_OR_UNMAPPED"),
        ),
        f.when(
            f.col(
                "_is_payment_type_missing_or_unmapped"
            ),
            f.lit(
                "PAYMENT_TYPE_MISSING_OR_UNMAPPED"
            ),
        ),
        f.when(
            f.col("_is_cross_year_trip"),
            f.lit("CROSS_YEAR_TRIP"),
        ),
        f.when(
            f.col(
                "_is_tip_reported_outside_supported_payment"
            ),
            f.lit(
                "TIP_REPORTED_OUTSIDE_SUPPORTED_PAYMENT"
            ),
        ),
    )

    return f.array_sort(
        f.filter(
            reasons,
            lambda reason: reason.isNotNull(),
        )
    )


def _warning_array():
    return f.coalesce(
        f.col("_dq_warning_reasons"),
        f.expr("cast(array() as array<string>)"),
    )


def _has_warning(warning_name):
    return f.coalesce(
        f.array_contains(
            _warning_array(),
            warning_name,
        ),
        f.lit(False),
    )


def _has_any_warning(warning_names):
    condition = f.lit(False)

    for warning_name in warning_names:
        condition = condition | _has_warning(
            warning_name
        )

    return f.coalesce(
        condition,
        f.lit(False),
    )


def _eligibility_reasons():
    reasons = f.array(
        f.when(
            f.col("_is_quarantined"),
            f.lit("CRITICAL_QUALITY_VIOLATION"),
        ),
        f.when(
            ~f.col("_is_passenger_metric_eligible"),
            f.lit("PASSENGER_METRICS_INELIGIBLE"),
        ),
        f.when(
            ~f.col("_is_distance_metric_eligible"),
            f.lit("DISTANCE_METRICS_INELIGIBLE"),
        ),
        f.when(
            ~f.col("_is_duration_metric_eligible"),
            f.lit("DURATION_METRICS_INELIGIBLE"),
        ),
        f.when(
            ~f.col(
                "_is_financial_breakdown_metric_eligible"
            ),
            f.lit(
                "FINANCIAL_BREAKDOWN_INELIGIBLE"
            ),
        ),
        f.when(
            ~f.col("_is_reported_tip_metric_eligible"),
            f.lit(
                "REPORTED_TIP_METRICS_INELIGIBLE"
            ),
        ),
        f.when(
            ~f.col("_is_rate_code_metric_eligible"),
            f.lit("RATE_CODE_METRICS_INELIGIBLE"),
        ),
        f.when(
            ~f.col("_is_vendor_metric_eligible"),
            f.lit("VENDOR_METRICS_INELIGIBLE"),
        ),
        f.when(
            ~f.col(
                "_is_payment_type_metric_eligible"
            ),
            f.lit(
                "PAYMENT_TYPE_METRICS_INELIGIBLE"
            ),
        ),
        f.when(
            ~f.col("_is_route_metric_eligible"),
            f.lit("ROUTE_METRICS_INELIGIBLE"),
        ),
        f.when(
            f.col("_is_reversal_or_adjustment"),
            f.lit(
                "RECONCILED_REVERSAL_OR_ADJUSTMENT"
            ),
        ),
        f.when(
            f.col("_financial_record_type")
            == f.lit(
                "NEGATIVE_COMPONENT_REVIEW"
            ),
            f.lit(
                "NEGATIVE_COMPONENT_REQUIRES_REVIEW"
            ),
        ),
        f.when(
            _has_warning(DUPLICATE_WARNING),
            f.lit(
                "SOURCE_DUPLICATE_CANONICAL_RECORD"
            ),
        ),
        f.when(
            f.col("_requires_gold_date_extension"),
            f.lit("GOLD_DATE_EXTENSION_REQUIRED"),
        ),
    )

    return f.array_sort(
        f.filter(
            reasons,
            lambda reason: reason.isNotNull(),
        )
    )


def _add_eligibility_columns(dataframe):
    source_is_valid = f.coalesce(
        ~f.col("_is_quarantined"),
        f.lit(False),
    )

    passenger_eligible = (
        source_is_valid
        & (~_has_any_warning(PASSENGER_WARNINGS))
    )

    distance_eligible = (
        source_is_valid
        & (~_has_any_warning(DISTANCE_WARNINGS))
        & f.coalesce(
            f.col("trip_distance") > f.lit(0),
            f.lit(False),
        )
    )

    duration_eligible = (
        source_is_valid
        & f.coalesce(
            f.col("trip_duration_minutes")
            > f.lit(0),
            f.lit(False),
        )
        & (~f.coalesce(
            f.col("_is_long_trip"),
            f.lit(True),
        ))
    )

    recorded_amount_eligible = (
        source_is_valid
        & f.col("total_amount").isNotNull()
    )

    financial_breakdown_eligible = (
        recorded_amount_eligible
        & (~_has_any_warning(
            FINANCIAL_RECONCILIATION_WARNINGS
        ))
    )

    rate_code_eligible = (
        source_is_valid
        & (~_has_any_warning(RATE_CODE_WARNINGS))
    )

    vendor_eligible = (
        source_is_valid
        & (~_has_any_warning(VENDOR_WARNINGS))
    )

    payment_type_eligible = (
        source_is_valid
        & (~_has_any_warning(
            PAYMENT_TYPE_WARNINGS
        ))
    )

    route_eligible = (
        source_is_valid
        & f.col(
            "pickup_zone_location_id"
        ).isNotNull()
        & f.col(
            "dropoff_zone_location_id"
        ).isNotNull()
    )

    reported_tip_eligible = (
        recorded_amount_eligible
        & payment_type_eligible
        & f.coalesce(
            f.col("payment_type").isin(0, 1),
            f.lit(False),
        )
        & (~_has_any_warning(TIP_WARNINGS))
    )

    negative_financial_record = (
        _has_any_warning(
            NEGATIVE_FINANCIAL_WARNINGS
        )
    )

    negative_total_record = _has_warning(
        "NEGATIVE_TOTAL_AMOUNT"
    )

    financial_gap_record = _has_warning(
        "FINANCIAL_RECONCILIATION_GAP"
    )

    reversal_or_adjustment = (
        negative_total_record
        & negative_financial_record
        & (~financial_gap_record)
    )

    unusual_negative_component = (
        negative_financial_record
        & (~negative_total_record)
    )

    financial_feature_eligible = (
        financial_breakdown_eligible
        & (~negative_financial_record)
    )

    categorical_feature_eligible = (
        rate_code_eligible
        & vendor_eligible
        & payment_type_eligible
    )

    efficiency_eligible = (
        distance_eligible
        & duration_eligible
    )

    standard_operational_trip_eligible = (
        efficiency_eligible
        & (~negative_total_record)
        & (~_has_warning(DUPLICATE_WARNING))
    )

    ml_standard_trip_eligible = (
        standard_operational_trip_eligible
        & passenger_eligible
        & financial_feature_eligible
        & categorical_feature_eligible
    )

    requires_data_review = (
        f.col("_is_quarantined")
        | _has_any_warning(REVIEW_WARNINGS)
        | unusual_negative_component
    )

    return (
        dataframe
        .withColumn(
            "_passenger_data_status",
            f.when(
                _has_warning(
                    "FLEX_FARE_PASSENGER_COUNT_MISSING"
                ),
                f.lit(
                    "EXPECTED_FLEX_FARE_MISSING"
                ),
            )
            .when(
                _has_warning(
                    "PASSENGER_COUNT_MISSING"
                ),
                f.lit("MISSING_UNEXPECTED"),
            )
            .when(
                f.col("_is_negative_passenger_count"),
                f.lit("NEGATIVE_INVALID"),
            )
            .when(
                f.col("_is_zero_passenger_count"),
                f.lit("ZERO_REPORTED"),
            )
            .otherwise(
                f.lit("REPORTED_VALID")
            ),
        )
        .withColumn(
            "_distance_data_status",
            f.when(
                f.col("trip_distance") < f.lit(0),
                f.lit("NEGATIVE_INVALID"),
            )
            .when(
                _has_warning("ZERO_TRIP_DISTANCE")
                & negative_total_record,
                f.lit("ZERO_DISTANCE_REVERSAL"),
            )
            .when(
                _has_warning("ZERO_TRIP_DISTANCE")
                & f.coalesce(
                    f.col("total_amount")
                    == f.lit(0),
                    f.lit(False),
                ),
                f.lit("ZERO_DISTANCE_NO_CHARGE"),
            )
            .when(
                _has_warning("ZERO_TRIP_DISTANCE"),
                f.lit("ZERO_DISTANCE_WITH_CHARGE"),
            )
            .otherwise(
                f.lit("POSITIVE_DISTANCE")
            ),
        )
        .withColumn(
            "_financial_record_type",
            f.when(
                financial_gap_record
                & negative_total_record,
                f.lit("NEGATIVE_UNRECONCILED"),
            )
            .when(
                financial_gap_record,
                f.lit("UNRECONCILED"),
            )
            .when(
                reversal_or_adjustment,
                f.lit(
                    "RECONCILED_REVERSAL_OR_ADJUSTMENT"
                ),
            )
            .when(
                unusual_negative_component,
                f.lit(
                    "NEGATIVE_COMPONENT_REVIEW"
                ),
            )
            .otherwise(
                f.lit("REGULAR_TRANSACTION")
            ),
        )
        .withColumn(
            "_is_trip_volume_metric_eligible",
            source_is_valid,
        )
        .withColumn(
            "_is_passenger_metric_eligible",
            passenger_eligible,
        )
        .withColumn(
            "_is_distance_metric_eligible",
            distance_eligible,
        )
        .withColumn(
            "_is_duration_metric_eligible",
            duration_eligible,
        )
        .withColumn(
            "_is_efficiency_metric_eligible",
            efficiency_eligible,
        )
        .withColumn(
            "_is_recorded_amount_metric_eligible",
            recorded_amount_eligible,
        )
        .withColumn(
            "_is_financial_breakdown_metric_eligible",
            financial_breakdown_eligible,
        )
        .withColumn(
            "_is_reported_tip_metric_eligible",
            reported_tip_eligible,
        )
        .withColumn(
            "_is_rate_code_metric_eligible",
            rate_code_eligible,
        )
        .withColumn(
            "_is_vendor_metric_eligible",
            vendor_eligible,
        )
        .withColumn(
            "_is_payment_type_metric_eligible",
            payment_type_eligible,
        )
        .withColumn(
            "_is_route_metric_eligible",
            route_eligible,
        )
        .withColumn(
            "_is_reversal_or_adjustment",
            reversal_or_adjustment,
        )
        .withColumn(
            "_is_ml_distance_feature_eligible",
            distance_eligible,
        )
        .withColumn(
            "_is_ml_passenger_feature_eligible",
            passenger_eligible,
        )
        .withColumn(
            "_is_ml_financial_feature_eligible",
            financial_feature_eligible,
        )
        .withColumn(
            "_is_ml_categorical_feature_eligible",
            categorical_feature_eligible,
        )
        .withColumn(
            "_is_standard_operational_trip_eligible",
            standard_operational_trip_eligible,
        )
        .withColumn(
            "_is_ml_standard_trip_eligible",
            ml_standard_trip_eligible,
        )
        .withColumn(
            "_requires_gold_date_extension",
            _has_any_warning(CROSS_YEAR_WARNINGS),
        )
        .withColumn(
            "_requires_data_review",
            requires_data_review,
        )
        .withColumn(
            "_eligibility_reasons",
            _eligibility_reasons(),
        )
        .withColumn(
            "_eligibility_restriction_count",
            f.size("_eligibility_reasons"),
        )
        .withColumn(
            "_eligibility_status",
            f.when(
                f.col("_requires_data_review"),
                f.lit("REVIEW_REQUIRED"),
            )
            .when(
                f.col("_is_ml_standard_trip_eligible")
                & (
                    f.col(
                        "_eligibility_restriction_count"
                    )
                    == f.lit(0)
                ),
                f.lit("FULLY_ELIGIBLE"),
            )
            .otherwise(
                f.lit("PARTIALLY_ELIGIBLE")
            ),
        )
    )


@dp.temporary_view(
    name="silver_yellow_trip_classified"
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
                f.trim("store_and_fwd_flag")
            ).alias("store_and_fwd_flag"),
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
        .withColumn(
            "store_and_forward_flag",
            f.col("store_and_fwd_flag"),
        )
    )

    zones_df = (
        spark.read.table(TAXI_ZONE_LOOKUP_SOURCE)
        .select(
            "location_id",
            "borough",
            "zone",
            "service_zone",
        )
    )

    pickup_zones_df = zones_df.select(
        f.col("location_id").alias(
            "pickup_zone_location_id"
        ),
        f.col("borough").alias(
            "pickup_borough"
        ),
        f.col("zone").alias("pickup_zone"),
        f.col("service_zone").alias(
            "pickup_service_zone"
        ),
    )

    dropoff_zones_df = zones_df.select(
        f.col("location_id").alias(
            "dropoff_zone_location_id"
        ),
        f.col("borough").alias(
            "dropoff_borough"
        ),
        f.col("zone").alias("dropoff_zone"),
        f.col("service_zone").alias(
            "dropoff_service_zone"
        ),
    )

    duplicate_window = Window.partitionBy(
        "_record_hash"
    )
    duplicate_order_window = (
        duplicate_window.orderBy(
            f.col("_record_hash")
        )
    )

    classified_df = (
        trips_df
        .join(
            pickup_zones_df,
            f.col("pickup_location_id")
            == f.col("pickup_zone_location_id"),
            "left",
        )
        .join(
            dropoff_zones_df,
            f.col("dropoff_location_id")
            == f.col("dropoff_zone_location_id"),
            "left",
        )
        .withColumn(
            "pickup_date",
            f.to_date("pickup_datetime"),
        )
        .withColumn(
            "dropoff_date",
            f.to_date("dropoff_datetime"),
        )
        .withColumn(
            "pickup_year",
            f.year("pickup_datetime"),
        )
        .withColumn(
            "dropoff_year",
            f.year("dropoff_datetime"),
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
                    f.col("dropoff_datetime")
                    .cast("long")
                    - f.col("pickup_datetime")
                    .cast("long")
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
            "record_hash",
            f.col("_record_hash"),
        )
        .withColumn(
            "_duplicate_record_count",
            f.count(f.lit(1)).over(
                duplicate_window
            ),
        )
        .withColumn(
            "_duplicate_occurrence_number",
            f.row_number().over(
                duplicate_order_window
            ),
        )
        .withColumn(
            "_is_duplicate_group",
            f.col("_duplicate_record_count")
            > f.lit(1),
        )
        .withColumn(
            "_is_duplicate_record",
            f.col("_duplicate_occurrence_number")
            > f.lit(1),
        )
        .withColumn(
            "_dq_reasons",
            _critical_quality_reasons(),
        )
        .withColumn(
            "_is_quarantined",
            f.size("_dq_reasons") > f.lit(0),
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
            "_is_zero_passenger_count",
            f.coalesce(
                f.col("passenger_count") == f.lit(0),
                f.lit(False),
            ),
        )
        .withColumn(
            "_is_negative_passenger_count",
            f.coalesce(
                f.col("passenger_count") < f.lit(0),
                f.lit(False),
            ),
        )
        .withColumn(
            "_is_non_positive_passenger_count",
            f.coalesce(
                f.col("passenger_count") <= f.lit(0),
                f.lit(False),
            ),
        )
        .withColumn(
            "_is_flex_fare",
            f.coalesce(
                f.col("payment_type") == f.lit(0),
                f.lit(False),
            ),
        )
        .withColumn(
            "_is_expected_flex_fare_passenger_gap",
            f.col("_is_flex_fare")
            & f.col("_is_passenger_count_missing"),
        )
        .withColumn(
            "_is_unexpected_passenger_count_missing",
            (~f.col("_is_flex_fare"))
            & f.col("_is_passenger_count_missing"),
        )
        .withColumn(
            "_is_negative_total_amount",
            f.coalesce(
                f.col("total_amount") < f.lit(0),
                f.lit(False),
            ),
        )
        .withColumn(
            "_is_negative_fare_amount",
            f.coalesce(
                f.col("fare_amount") < f.lit(0),
                f.lit(False),
            ),
        )
        .withColumn(
            "_is_negative_tip_amount",
            f.coalesce(
                f.col("tip_amount") < f.lit(0),
                f.lit(False),
            ),
        )
        .withColumn(
            "_has_negative_financial_component",
            f.coalesce(
                (f.col("fare_amount") < f.lit(0))
                | (f.col("extra_amount") < f.lit(0))
                | (f.col("mta_tax_amount") < f.lit(0))
                | (f.col("tip_amount") < f.lit(0))
                | (f.col("tolls_amount") < f.lit(0))
                | (
                    f.col(
                        "improvement_surcharge_amount"
                    )
                    < f.lit(0)
                )
                | (
                    f.col(
                        "congestion_surcharge_amount"
                    )
                    < f.lit(0)
                )
                | (
                    f.col("airport_fee_amount")
                    < f.lit(0)
                )
                | (
                    f.col("cbd_congestion_fee_amount")
                    < f.lit(0)
                ),
                f.lit(False),
            ),
        )
        .withColumn(
            "_financial_component_amount",
            _financial_component_amount(),
        )
        .withColumn(
            "_financial_reconciliation_difference",
            (
                f.col("total_amount")
                .cast("decimal(20, 2)")
                - f.col(
                    "_financial_component_amount"
                )
            ).cast("decimal(20, 2)"),
        )
        .withColumn(
            "_is_financially_unreconciled",
            f.coalesce(
                f.abs(
                    f.col(
                        "_financial_reconciliation_difference"
                    )
                )
                > f.lit(
                    FINANCIAL_RECONCILIATION_TOLERANCE
                ).cast("decimal(20, 4)"),
                f.lit(False),
            ),
        )
        .withColumn(
            "_is_rate_code_missing_or_unmapped",
            f.col("rate_code_id").isNull()
            | (~f.col("rate_code_id").isin(
                *VALID_RATE_CODE_IDS
            )),
        )
        .withColumn(
            "_is_expected_flex_fare_rate_gap",
            f.col("_is_flex_fare")
            & f.col(
                "_is_rate_code_missing_or_unmapped"
            ),
        )
        .withColumn(
            "_is_unexpected_rate_code_gap",
            (~f.col("_is_flex_fare"))
            & f.col(
                "_is_rate_code_missing_or_unmapped"
            ),
        )
        .withColumn(
            "_is_vendor_missing_or_unmapped",
            f.col("vendor_id").isNull()
            | (~f.col("vendor_id").isin(
                *VALID_VENDOR_IDS
            )),
        )
        .withColumn(
            "_is_payment_type_missing_or_unmapped",
            f.col("payment_type").isNull()
            | (~f.col("payment_type").isin(
                *VALID_PAYMENT_TYPE_CODES
            )),
        )
        .withColumn(
            "_is_cross_year_trip",
            f.coalesce(
                f.col("pickup_year")
                != f.col("dropoff_year"),
                f.lit(False),
            ),
        )
        .withColumn(
            "_is_tip_reported_outside_supported_payment",
            f.coalesce(
                (f.col("tip_amount") > f.lit(0))
                & (~f.coalesce(
                    f.col("payment_type").isin(0, 1),
                    f.lit(False),
                )),
                f.lit(False),
            ),
        )
        .withColumn(
            "_is_long_trip",
            f.coalesce(
                f.col("trip_duration_minutes")
                > f.lit(
                    MAX_VALID_TRIP_DURATION_MINUTES
                ),
                f.lit(False),
            ),
        )
        .withColumn(
            "_is_efficiency_kpi_eligible",
            f.coalesce(
                (~f.col("_is_quarantined"))
                & (f.col("trip_distance") > f.lit(0))
                & (
                    f.col("trip_duration_minutes")
                    > f.lit(0)
                )
                & (
                    f.col("trip_duration_minutes")
                    <= f.lit(
                        MAX_VALID_TRIP_DURATION_MINUTES
                    )
                ),
                f.lit(False),
            ),
        )
        .withColumn(
            "_dq_warning_reasons",
            _warning_reasons(),
        )
        .withColumn(
            "_dq_warning_count",
            f.size("_dq_warning_reasons"),
        )
        .withColumn(
            "_has_dq_warnings",
            f.col("_dq_warning_count") > f.lit(0),
        )
    )

    return (
        _add_eligibility_columns(classified_df)
        .withColumn(
            "_quality_rule_version",
            f.lit(QUALITY_RULE_VERSION),
        )
        .withColumn(
            "_silver_refresh_timestamp",
            f.current_timestamp(),
        )
        .withColumn(
            "_eligibility_refresh_timestamp",
            f.current_timestamp(),
        )
    )


@dp.materialized_view(
    name="silver_yellow_trip_2025",
    comment=(
        "Viagens NYC Yellow Taxi validadas, "
        "enriquecidas e classificadas por "
        "elegibilidade analítica e de ML."
    ),
    cluster_by_auto=True,
)
@dp.expect_all_or_fail(
    PUBLISHED_SILVER_EXPECTATIONS
)
def silver_yellow_trip_2025():
    return (
        spark.read.table(
            "silver_yellow_trip_classified"
        )
        .where(~f.col("_is_quarantined"))
    )


@dp.materialized_view(
    name="quarantine_yellow_trip_2025",
    comment=(
        "Viagens rejeitadas pelo contrato "
        "crítico de qualidade da camada Silver."
    ),
    cluster_by_auto=True,
)
@dp.expect_all_or_fail(
    QUARANTINE_EXPECTATIONS
)
def quarantine_yellow_trip_2025():
    return (
        spark.read.table(
            "silver_yellow_trip_classified"
        )
        .where(f.col("_is_quarantined"))
    )
