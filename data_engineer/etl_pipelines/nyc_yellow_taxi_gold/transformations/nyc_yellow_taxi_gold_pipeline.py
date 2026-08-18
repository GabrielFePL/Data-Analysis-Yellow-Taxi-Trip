"""Lakeflow Declarative Pipeline - NYC Yellow Taxi Gold star schema.

Pipeline target:
    catalog: nyc_taxi
    schema:  gold

Default upstream assets:
    nyc_taxi.silver.silver_yellow_trip_2025
    nyc_taxi.silver.silver_taxi_zone_lookup

The pipeline publishes seven conformed dimensions and one trip-grain fact.
The fact consumes the Silver v2 quality and eligibility contract without
reclassifying records that have already passed critical Silver validation.
"""

from pyspark import pipelines as dp
import pyspark.sql.functions as f


# Pipeline configuration can override these defaults without changing source code.
TRIP_SOURCE_TABLE = spark.conf.get(
    "nyc_taxi.gold.trip_source_table",
    "nyc_taxi.silver.silver_yellow_trip_2025",
)
ZONE_SOURCE_TABLE = spark.conf.get(
    "nyc_taxi.gold.zone_source_table",
    "nyc_taxi.silver.silver_taxi_zone_lookup",
)
SILVER_QUALITY_RULE_VERSION = "2.0"


MONTH_NAMES = [
    "January",
    "February",
    "March",
    "April",
    "May",
    "June",
    "July",
    "August",
    "September",
    "October",
    "November",
    "December",
]

DAY_NAMES = [
    "Monday",
    "Tuesday",
    "Wednesday",
    "Thursday",
    "Friday",
    "Saturday",
    "Sunday",
]

SILVER_SOURCE_EXPECTATIONS = {
    "source_record_hash_present": (
        "source_record_hash IS NOT NULL"
    ),
    "source_record_is_not_quarantined": (
        "is_quarantined = false"
    ),
    "source_has_no_critical_reason": (
        "size(critical_quality_reasons) = 0"
    ),
    "source_trip_volume_is_eligible": (
        "is_trip_volume_metric_eligible = true"
    ),
    "source_recorded_amount_is_eligible": (
        "is_recorded_amount_metric_eligible = true"
    ),
    "source_route_is_eligible": (
        "is_route_metric_eligible = true"
    ),
    "source_quality_rule_is_supported": (
        "quality_rule_version = "
        f"'{SILVER_QUALITY_RULE_VERSION}'"
    ),
}


DIM_DATE_EXPECTATIONS = {
    "date_member_is_known_or_unknown": (
        "(date_key = 0 AND full_date IS NULL) "
        "OR (date_key > 0 AND full_date IS NOT NULL)"
    ),
    "known_date_key_matches_date": (
        "date_key = 0 OR date_key = "
        "CAST(date_format(full_date, 'yyyyMMdd') AS INT)"
    ),
    "calendar_year_role_is_valid": (
        "calendar_year_role IN ("
        "'PREDECESSOR_YEAR', "
        "'SOURCE_YEAR_RANGE', "
        "'SUCCESSOR_YEAR', "
        "'UNKNOWN')"
    ),
}


FACT_EXPECTATIONS = {
    "trip_key_present": "trip_key IS NOT NULL",
    "pickup_date_resolved": "pickup_date_key > 0",
    "dropoff_date_resolved": "dropoff_date_key > 0",
    "pickup_time_resolved": "pickup_time_key >= 0",
    "dropoff_time_resolved": "dropoff_time_key >= 0",
    "pickup_zone_resolved": "pickup_zone_key > 0",
    "dropoff_zone_resolved": "dropoff_zone_key > 0",
    "trip_volume_is_eligible": (
        "is_trip_volume_metric_eligible = true"
    ),
    "recorded_amount_is_eligible": (
        "is_recorded_amount_metric_eligible = true"
    ),
    "route_is_eligible": "is_route_metric_eligible = true",
    "warning_count_is_consistent": (
        "dq_warning_count = size(dq_warning_reasons)"
    ),
    "eligibility_count_is_consistent": (
        "eligibility_restriction_count = "
        "size(eligibility_reasons)"
    ),
    "efficiency_eligibility_is_consistent": (
        "is_efficiency_metric_eligible = ("
        "is_distance_metric_eligible AND "
        "is_duration_metric_eligible)"
    ),
    "ml_standard_trip_is_consistent": (
        "is_ml_standard_trip_eligible = ("
        "is_standard_operational_trip_eligible AND "
        "is_passenger_metric_eligible AND "
        "is_ml_financial_feature_eligible AND "
        "is_ml_categorical_feature_eligible)"
    ),
}


def _source_column(
    dataframe,
    candidates,
    alias,
    data_type,
    required=True,
    default_value=None,
):
    """Resolve Silver canonical names and TLC raw-name variants without actions."""
    source_names = {column_name.lower(): column_name for column_name in dataframe.columns}
    resolved_name = next(
        (
            source_names[candidate.lower()]
            for candidate in candidates
            if candidate.lower() in source_names
        ),
        None,
    )

    if resolved_name is None:
        if required:
            raise ValueError(
                f"Required column for '{alias}' was not found. "
                f"Accepted names: {candidates}. Available columns: {dataframe.columns}"
            )
        return f.lit(default_value).cast(data_type).alias(alias)

    return f.col(f"`{resolved_name}`").cast(data_type).alias(alias)


def _silver_contract_column(
    dataframe,
    source_name,
    alias,
    data_type,
):
    """Resolve a required field from the consolidated Silver contract."""
    return _source_column(
        dataframe,
        [source_name],
        alias,
        data_type,
        required=True,
    )


def _us_federal_holiday_name(date_column):
    """Return the federal-holiday name for any calendar year.

    The rules identify the statutory holiday date. Observed weekdays are not
    substituted when a fixed-date holiday falls on a weekend.
    """
    month_number = f.month(date_column)
    day_number = f.dayofmonth(date_column)
    weekday_number = f.dayofweek(date_column)

    return (
        f.when(
            (month_number == 1) & (day_number == 1),
            f.lit("New Year's Day"),
        )
        .when(
            (month_number == 1)
            & (weekday_number == 2)
            & day_number.between(15, 21),
            f.lit("Martin Luther King Jr. Day"),
        )
        .when(
            (month_number == 2)
            & (weekday_number == 2)
            & day_number.between(15, 21),
            f.lit("Washington's Birthday"),
        )
        .when(
            (month_number == 5)
            & (weekday_number == 2)
            & day_number.between(25, 31),
            f.lit("Memorial Day"),
        )
        .when(
            (month_number == 6) & (day_number == 19),
            f.lit("Juneteenth National Independence Day"),
        )
        .when(
            (month_number == 7) & (day_number == 4),
            f.lit("Independence Day"),
        )
        .when(
            (month_number == 9)
            & (weekday_number == 2)
            & day_number.between(1, 7),
            f.lit("Labor Day"),
        )
        .when(
            (month_number == 10)
            & (weekday_number == 2)
            & day_number.between(8, 14),
            f.lit("Columbus Day"),
        )
        .when(
            (month_number == 11) & (day_number == 11),
            f.lit("Veterans Day"),
        )
        .when(
            (month_number == 11)
            & (weekday_number == 5)
            & day_number.between(22, 28),
            f.lit("Thanksgiving Day"),
        )
        .when(
            (month_number == 12) & (day_number == 25),
            f.lit("Christmas Day"),
        )
    )


@dp.temporary_view(name="gold_trip_source")
@dp.expect_all_or_fail(SILVER_SOURCE_EXPECTATIONS)
def gold_trip_source():
    """Canonical projection of the validated Silver v2 trip asset."""
    source = spark.read.table(TRIP_SOURCE_TABLE)

    return source.select(
        _source_column(
            source,
            ["vendor_id", "VendorID"],
            "vendor_id",
            "int",
        ),
        _source_column(
            source,
            ["pickup_datetime", "tpep_pickup_datetime"],
            "pickup_datetime",
            "timestamp",
        ),
        _source_column(
            source,
            ["dropoff_datetime", "tpep_dropoff_datetime"],
            "dropoff_datetime",
            "timestamp",
        ),
        _source_column(
            source,
            ["passenger_count"],
            "passenger_count",
            "int",
        ),
        _source_column(
            source,
            ["trip_distance_miles", "trip_distance"],
            "trip_distance_miles",
            "decimal(18,3)",
        ),
        _silver_contract_column(
            source,
            "trip_duration_minutes",
            "trip_duration_minutes",
            "decimal(12,2)",
        ),
        _source_column(
            source,
            ["rate_code_id", "ratecode_id", "RatecodeID"],
            "rate_code_id",
            "int",
        ),
        _source_column(
            source,
            ["store_and_fwd_flag", "store_and_forward_flag"],
            "store_and_fwd_flag",
            "string",
        ),
        _source_column(
            source,
            ["pickup_location_id", "PULocationID"],
            "pickup_location_id",
            "int",
        ),
        _source_column(
            source,
            ["dropoff_location_id", "DOLocationID"],
            "dropoff_location_id",
            "int",
        ),
        _source_column(
            source,
            ["payment_type_id", "payment_type"],
            "payment_type_id",
            "int",
        ),
        _source_column(
            source,
            ["fare_amount"],
            "fare_amount",
            "decimal(18,2)",
        ),
        _source_column(
            source,
            ["extra_amount", "extra"],
            "extra_amount",
            "decimal(18,2)",
        ),
        _source_column(
            source,
            ["mta_tax_amount", "mta_tax"],
            "mta_tax_amount",
            "decimal(18,2)",
        ),
        _source_column(
            source,
            ["tip_amount"],
            "tip_amount",
            "decimal(18,2)",
        ),
        _source_column(
            source,
            ["tolls_amount"],
            "tolls_amount",
            "decimal(18,2)",
        ),
        _source_column(
            source,
            ["improvement_surcharge_amount", "improvement_surcharge"],
            "improvement_surcharge_amount",
            "decimal(18,2)",
        ),
        _source_column(
            source,
            ["total_amount"],
            "total_amount",
            "decimal(18,2)",
        ),
        _source_column(
            source,
            ["congestion_surcharge_amount", "congestion_surcharge"],
            "congestion_surcharge_amount",
            "decimal(18,2)",
        ),
        _source_column(
            source,
            ["airport_fee_amount", "airport_fee", "Airport_fee"],
            "airport_fee_amount",
            "decimal(18,2)",
        ),
        _source_column(
            source,
            ["cbd_congestion_fee_amount", "cbd_congestion_fee"],
            "cbd_congestion_fee_amount",
            "decimal(18,2)",
        ),
        _source_column(
            source,
            ["record_hash", "trip_record_hash"],
            "source_record_hash",
            "string",
        ),
        _source_column(
            source,
            ["source_year"],
            "source_year",
            "int",
        ),
        _source_column(
            source,
            ["source_month"],
            "source_month",
            "int",
        ),
        _silver_contract_column(
            source,
            "_financial_component_amount",
            "financial_component_amount",
            "decimal(20,2)",
        ),
        _silver_contract_column(
            source,
            "_financial_reconciliation_difference",
            "financial_reconciliation_difference",
            "decimal(20,2)",
        ),
        _silver_contract_column(
            source,
            "_dq_reasons",
            "critical_quality_reasons",
            "array<string>",
        ),
        _silver_contract_column(
            source,
            "_dq_warning_reasons",
            "dq_warning_reasons",
            "array<string>",
        ),
        _silver_contract_column(
            source,
            "_dq_warning_count",
            "dq_warning_count",
            "int",
        ),
        _silver_contract_column(
            source,
            "_has_dq_warnings",
            "has_dq_warnings",
            "boolean",
        ),
        _silver_contract_column(
            source,
            "_passenger_data_status",
            "passenger_data_status",
            "string",
        ),
        _silver_contract_column(
            source,
            "_distance_data_status",
            "distance_data_status",
            "string",
        ),
        _silver_contract_column(
            source,
            "_financial_record_type",
            "financial_record_type",
            "string",
        ),
        _silver_contract_column(
            source,
            "_eligibility_reasons",
            "eligibility_reasons",
            "array<string>",
        ),
        _silver_contract_column(
            source,
            "_eligibility_restriction_count",
            "eligibility_restriction_count",
            "int",
        ),
        _silver_contract_column(
            source,
            "_eligibility_status",
            "eligibility_status",
            "string",
        ),
        _silver_contract_column(
            source,
            "_quality_rule_version",
            "quality_rule_version",
            "string",
        ),
        _silver_contract_column(
            source,
            "_silver_refresh_timestamp",
            "silver_refresh_timestamp",
            "timestamp",
        ),
        _silver_contract_column(
            source,
            "_eligibility_refresh_timestamp",
            "eligibility_refresh_timestamp",
            "timestamp",
        ),
        *[
            _silver_contract_column(
                source,
                source_name,
                alias,
                "boolean",
            )
            for source_name, alias in [
                ("_is_quarantined", "is_quarantined"),
                ("_is_flex_fare", "is_flex_fare"),
                ("_is_zero_distance", "is_zero_distance"),
                (
                    "_is_passenger_count_missing",
                    "is_passenger_count_missing",
                ),
                (
                    "_is_zero_passenger_count",
                    "is_zero_passenger_count",
                ),
                (
                    "_is_financially_unreconciled",
                    "is_financially_unreconciled",
                ),
                (
                    "_is_negative_total_amount",
                    "is_negative_total_amount",
                ),
                (
                    "_has_negative_financial_component",
                    "has_negative_financial_component",
                ),
                ("_is_cross_year_trip", "is_cross_year_trip"),
                (
                    "_is_trip_volume_metric_eligible",
                    "is_trip_volume_metric_eligible",
                ),
                (
                    "_is_passenger_metric_eligible",
                    "is_passenger_metric_eligible",
                ),
                (
                    "_is_distance_metric_eligible",
                    "is_distance_metric_eligible",
                ),
                (
                    "_is_duration_metric_eligible",
                    "is_duration_metric_eligible",
                ),
                (
                    "_is_efficiency_metric_eligible",
                    "is_efficiency_metric_eligible",
                ),
                (
                    "_is_efficiency_kpi_eligible",
                    "is_efficiency_kpi_eligible",
                ),
                (
                    "_is_recorded_amount_metric_eligible",
                    "is_recorded_amount_metric_eligible",
                ),
                (
                    "_is_financial_breakdown_metric_eligible",
                    "is_financial_breakdown_metric_eligible",
                ),
                (
                    "_is_reported_tip_metric_eligible",
                    "is_reported_tip_metric_eligible",
                ),
                (
                    "_is_rate_code_metric_eligible",
                    "is_rate_code_metric_eligible",
                ),
                (
                    "_is_vendor_metric_eligible",
                    "is_vendor_metric_eligible",
                ),
                (
                    "_is_payment_type_metric_eligible",
                    "is_payment_type_metric_eligible",
                ),
                (
                    "_is_route_metric_eligible",
                    "is_route_metric_eligible",
                ),
                (
                    "_is_reversal_or_adjustment",
                    "is_reversal_or_adjustment",
                ),
                (
                    "_is_standard_operational_trip_eligible",
                    "is_standard_operational_trip_eligible",
                ),
                (
                    "_is_ml_distance_feature_eligible",
                    "is_ml_distance_feature_eligible",
                ),
                (
                    "_is_ml_passenger_feature_eligible",
                    "is_ml_passenger_feature_eligible",
                ),
                (
                    "_is_ml_financial_feature_eligible",
                    "is_ml_financial_feature_eligible",
                ),
                (
                    "_is_ml_categorical_feature_eligible",
                    "is_ml_categorical_feature_eligible",
                ),
                (
                    "_is_ml_standard_trip_eligible",
                    "is_ml_standard_trip_eligible",
                ),
                (
                    "_requires_data_review",
                    "requires_data_review",
                ),
                (
                    "_requires_gold_date_extension",
                    "requires_gold_date_extension",
                ),
            ]
        ],
    )


@dp.temporary_view(name="gold_zone_source")
def gold_zone_source():
    """Canonical projection of the validated Silver taxi-zone lookup."""
    source = spark.read.table(ZONE_SOURCE_TABLE)

    return (
        source.select(
            _source_column(
                source,
                ["location_id", "LocationID"],
                "location_id",
                "int",
            ),
            _source_column(source, ["borough"], "borough", "string"),
            _source_column(source, ["zone_name", "zone"], "zone_name", "string"),
            _source_column(
                source,
                ["service_zone"],
                "service_zone",
                "string",
                required=False,
                default_value="Unknown",
            ),
        )
        .dropDuplicates(["location_id"])
    )


@dp.materialized_view(
    name="dim_date",
    comment=(
        "Data-driven calendar at day grain. It includes the complete year "
        "before the minimum trip year and the complete year after the "
        "maximum trip year."
    ),
    table_properties={"quality": "gold", "data_product": "yellow_taxi_business"},
)
@dp.expect_all_or_fail(DIM_DATE_EXPECTATIONS)
def dim_date():
    date_bounds = (
        spark.read.table("gold_trip_source")
        .agg(
            f.min(f.to_date("pickup_datetime")).alias(
                "minimum_pickup_date"
            ),
            f.min(f.to_date("dropoff_datetime")).alias(
                "minimum_dropoff_date"
            ),
            f.max(f.to_date("pickup_datetime")).alias(
                "maximum_pickup_date"
            ),
            f.max(f.to_date("dropoff_datetime")).alias(
                "maximum_dropoff_date"
            ),
        )
        .select(
            f.least(
                "minimum_pickup_date",
                "minimum_dropoff_date",
            ).alias("minimum_source_date"),
            f.greatest(
                "maximum_pickup_date",
                "maximum_dropoff_date",
            ).alias("maximum_source_date"),
        )
        .withColumn(
            "minimum_source_year",
            f.year("minimum_source_date"),
        )
        .withColumn(
            "maximum_source_year",
            f.year("maximum_source_date"),
        )
        .withColumn(
            "calendar_start_date",
            f.make_date(
                f.col("minimum_source_year") - f.lit(1),
                f.lit(1),
                f.lit(1),
            ),
        )
        .withColumn(
            "calendar_end_date",
            f.make_date(
                f.col("maximum_source_year") + f.lit(1),
                f.lit(12),
                f.lit(31),
            ),
        )
    )

    calendar = date_bounds.select(
        "minimum_source_year",
        "maximum_source_year",
        f.explode(
            f.sequence(
                "calendar_start_date",
                "calendar_end_date",
            )
        ).alias("full_date"),
    )

    day_of_week_number = (
        f.pmod(f.dayofweek("full_date") + f.lit(5), f.lit(7)) + f.lit(1)
    ).cast("int")

    dated = calendar.withColumn(
        "holiday_name",
        _us_federal_holiday_name(f.col("full_date")),
    )

    dimension = dated.select(
        f.date_format("full_date", "yyyyMMdd").cast("int").alias("date_key"),
        f.col("full_date"),
        f.year("full_date").alias("year_number"),
        f.when(f.month("full_date") <= 6, f.lit(1)).otherwise(f.lit(2)).alias(
            "semester_number"
        ),
        f.quarter("full_date").alias("quarter_number"),
        f.concat(f.lit("Q"), f.quarter("full_date")).alias("quarter_name"),
        f.date_format("full_date", "yyyyMM").cast("int").alias("year_month_key"),
        f.date_format("full_date", "yyyy-MM").alias("year_month"),
        f.month("full_date").alias("month_number"),
        f.element_at(
            f.array(*[f.lit(month_name) for month_name in MONTH_NAMES]),
            f.month("full_date"),
        ).alias("month_name"),
        f.weekofyear("full_date").alias("week_of_year"),
        f.dayofmonth("full_date").alias("day_of_month"),
        day_of_week_number.alias("day_of_week_number"),
        f.element_at(
            f.array(*[f.lit(day_name) for day_name in DAY_NAMES]),
            day_of_week_number,
        ).alias("day_name"),
        day_of_week_number.isin(6, 7).alias("is_weekend"),
        f.col("holiday_name"),
        f.col("holiday_name").isNotNull().alias("is_federal_holiday"),
        f.when(f.col("holiday_name").isNotNull(), f.lit("US Federal")).alias(
            "holiday_scope"
        ),
        (
            (~day_of_week_number.isin(6, 7)) & f.col("holiday_name").isNull()
        ).alias("is_business_day"),
        f.when(
            f.year("full_date") < f.col("minimum_source_year"),
            f.lit("PREDECESSOR_YEAR"),
        )
        .when(
            f.year("full_date") > f.col("maximum_source_year"),
            f.lit("SUCCESSOR_YEAR"),
        )
        .otherwise(f.lit("SOURCE_YEAR_RANGE"))
        .alias("calendar_year_role"),
        f.year("full_date")
        .between(
            f.col("minimum_source_year"),
            f.col("maximum_source_year"),
        )
        .alias("is_source_year"),
    )

    unknown = spark.range(1).select(
        f.lit(0).cast("int").alias("date_key"),
        f.lit(None).cast("date").alias("full_date"),
        f.lit(None).cast("int").alias("year_number"),
        f.lit(None).cast("int").alias("semester_number"),
        f.lit(None).cast("int").alias("quarter_number"),
        f.lit("Unknown").alias("quarter_name"),
        f.lit(None).cast("int").alias("year_month_key"),
        f.lit("Unknown").alias("year_month"),
        f.lit(None).cast("int").alias("month_number"),
        f.lit("Unknown").alias("month_name"),
        f.lit(None).cast("int").alias("week_of_year"),
        f.lit(None).cast("int").alias("day_of_month"),
        f.lit(None).cast("int").alias("day_of_week_number"),
        f.lit("Unknown").alias("day_name"),
        f.lit(False).alias("is_weekend"),
        f.lit("Unknown").alias("holiday_name"),
        f.lit(False).alias("is_federal_holiday"),
        f.lit(None).cast("string").alias("holiday_scope"),
        f.lit(False).alias("is_business_day"),
        f.lit("UNKNOWN").alias("calendar_year_role"),
        f.lit(False).alias("is_source_year"),
    )

    return dimension.unionByName(unknown)


@dp.materialized_view(
    name="dim_time",
    comment="Time-of-day dimension at hour grain.",
    table_properties={"quality": "gold", "data_product": "yellow_taxi_business"},
)
def dim_time():
    hours = spark.range(0, 24).select(f.col("id").cast("int").alias("hour_number"))

    dimension = hours.select(
        f.col("hour_number").alias("time_key"),
        f.col("hour_number"),
        f.format_string("%02d:00-%02d:59", "hour_number", "hour_number").alias(
            "hour_range"
        ),
        f.when(f.col("hour_number").between(0, 5), f.lit("Overnight"))
        .when(f.col("hour_number").between(6, 11), f.lit("Morning"))
        .when(f.col("hour_number").between(12, 17), f.lit("Afternoon"))
        .otherwise(f.lit("Evening"))
        .alias("day_period"),
        f.when(f.col("hour_number").between(0, 5), f.lit("Overnight"))
        .when(f.col("hour_number").between(6, 9), f.lit("Morning peak"))
        .when(f.col("hour_number").between(10, 15), f.lit("Midday"))
        .when(f.col("hour_number").between(16, 19), f.lit("Evening peak"))
        .otherwise(f.lit("Late evening"))
        .alias("business_time_band"),
        (
            f.col("hour_number").between(6, 9)
            | f.col("hour_number").between(16, 19)
        ).alias("is_peak_hour"),
    )

    unknown = spark.range(1).select(
        f.lit(-1).cast("int").alias("time_key"),
        f.lit(None).cast("int").alias("hour_number"),
        f.lit("Unknown").alias("hour_range"),
        f.lit("Unknown").alias("day_period"),
        f.lit("Unknown").alias("business_time_band"),
        f.lit(False).alias("is_peak_hour"),
    )

    return dimension.unionByName(unknown)


def _zone_dimension(role):
    source = spark.read.table("gold_zone_source")
    key_name = f"{role}_zone_key"
    location_name = f"{role}_location_id"
    borough_name = f"{role}_borough"
    zone_name = f"{role}_zone_name"
    service_zone_name = f"{role}_service_zone"
    display_name = f"{role}_zone_display_name"

    dimension = source.select(
        f.col("location_id").alias(key_name),
        f.col("location_id").alias(location_name),
        f.col("borough").alias(borough_name),
        f.col("zone_name").alias(zone_name),
        f.col("service_zone").alias(service_zone_name),
        f.concat_ws(" - ", f.col("borough"), f.col("zone_name")).alias(display_name),
        f.col("location_id").isin(1, 132, 138).alias("is_airport_zone"),
        f.when(f.col("location_id") == 132, f.lit("JFK"))
        .when(f.col("location_id") == 138, f.lit("LGA"))
        .when(f.col("location_id") == 1, f.lit("EWR"))
        .otherwise(f.lit(None).cast("string"))
        .alias("airport_code"),
    )

    unknown = spark.range(1).select(
        f.lit(0).cast("int").alias(key_name),
        f.lit(0).cast("int").alias(location_name),
        f.lit("Unknown").alias(borough_name),
        f.lit("Unknown").alias(zone_name),
        f.lit("Unknown").alias(service_zone_name),
        f.lit("Unknown - Unknown").alias(display_name),
        f.lit(False).alias("is_airport_zone"),
        f.lit(None).cast("string").alias("airport_code"),
    )

    return dimension.unionByName(unknown)


@dp.materialized_view(
    name="dim_pickup_zone",
    comment="Role-playing taxi-zone dimension for trip pickup.",
    table_properties={"quality": "gold", "data_product": "yellow_taxi_business"},
)
def dim_pickup_zone():
    return _zone_dimension("pickup")


@dp.materialized_view(
    name="dim_dropoff_zone",
    comment="Role-playing taxi-zone dimension for trip dropoff.",
    table_properties={"quality": "gold", "data_product": "yellow_taxi_business"},
)
def dim_dropoff_zone():
    return _zone_dimension("dropoff")


@dp.materialized_view(
    name="dim_payment_type",
    comment="Official TLC payment-type codes with analytical groupings.",
    table_properties={"quality": "gold", "data_product": "yellow_taxi_business"},
)
def dim_payment_type():
    rows = [
        (-1, None, "Missing or unmapped", "Unknown", False, "Tip coverage unknown"),
        (0, 0, "Flex Fare trip", "Flex Fare", True, "Depends on payment channel"),
        (1, 1, "Credit card", "Electronic", True, "Electronic tips reported"),
        (2, 2, "Cash", "Cash", False, "Cash tips are not captured"),
        (3, 3, "No charge", "Non-revenue", False, "No passenger charge"),
        (4, 4, "Dispute", "Disputed", False, "Tip not expected"),
        (5, 5, "Unknown", "Unknown", False, "Tip coverage unknown"),
        (6, 6, "Voided trip", "Voided", False, "Tip not expected"),
    ]
    schema = """
        payment_type_key int,
        payment_type_code int,
        payment_type_name string,
        payment_category string,
        is_electronic_payment boolean,
        tip_data_note string
    """
    return spark.createDataFrame(rows, schema)


@dp.materialized_view(
    name="dim_rate_code",
    comment="Official TLC final-rate codes with business groupings.",
    table_properties={"quality": "gold", "data_product": "yellow_taxi_business"},
)
def dim_rate_code():
    rows = [
        (-1, None, "Missing or unmapped", "Unknown", None),
        (1, 1, "Standard rate", "Metered", None),
        (2, 2, "JFK", "Airport", "JFK"),
        (3, 3, "Newark", "Airport", "EWR"),
        (4, 4, "Nassau or Westchester", "Out-of-area", None),
        (5, 5, "Negotiated fare", "Negotiated", None),
        (6, 6, "Group ride", "Group", None),
        (99, 99, "Null or unknown", "Unknown", None),
    ]
    schema = """
        rate_code_key int,
        rate_code_id int,
        rate_code_name string,
        rate_category string,
        airport_code string
    """
    return spark.createDataFrame(rows, schema)


@dp.materialized_view(
    name="dim_vendor",
    comment="TPEP providers from the TLC Yellow Taxi data dictionary.",
    table_properties={"quality": "gold", "data_product": "yellow_taxi_business"},
)
def dim_vendor():
    rows = [
        (-1, None, "Missing or unmapped", "Unknown"),
        (1, 1, "Creative Mobile Technologies, LLC", "CMT"),
        (2, 2, "Curb Mobility, LLC", "Curb"),
        (6, 6, "Myle Technologies Inc", "Myle"),
        (7, 7, "Helix", "Helix"),
    ]
    schema = """
        vendor_key int,
        vendor_id int,
        vendor_name string,
        vendor_short_name string
    """
    return spark.createDataFrame(rows, schema)


@dp.materialized_view(
    name="fact_yellow_taxi_trip",
    comment=(
        "One row per Silver-validated Yellow Taxi trip, including quality and "
        "eligibility contracts. Financial amounts are passenger charges "
        "reported by TLC, not net revenue or profit."
    ),
    table_properties={"quality": "gold", "data_product": "yellow_taxi_business"},
    cluster_by=["pickup_date_key", "pickup_zone_key"],
)
@dp.expect_all_or_fail(FACT_EXPECTATIONS)
def fact_yellow_taxi_trip():
    trips = (
        spark.read.table("gold_trip_source")
        .withColumn("_pickup_date", f.to_date("pickup_datetime"))
        .withColumn("_dropoff_date", f.to_date("dropoff_datetime"))
        .withColumn("_pickup_hour", f.hour("pickup_datetime"))
        .withColumn("_dropoff_hour", f.hour("dropoff_datetime"))
        .alias("trip")
    )

    pickup_date = spark.read.table("dim_date").select(
        f.col("date_key").alias("pickup_date_key"),
        f.col("full_date").alias("pickup_full_date"),
    ).alias("pickup_date")
    dropoff_date = spark.read.table("dim_date").select(
        f.col("date_key").alias("dropoff_date_key"),
        f.col("full_date").alias("dropoff_full_date"),
    ).alias("dropoff_date")
    pickup_time = spark.read.table("dim_time").select(
        f.col("time_key").alias("pickup_time_key")
    ).alias("pickup_time")
    dropoff_time = spark.read.table("dim_time").select(
        f.col("time_key").alias("dropoff_time_key")
    ).alias("dropoff_time")
    pickup_zone = spark.read.table("dim_pickup_zone").select(
        "pickup_zone_key",
        "pickup_location_id",
        f.col("airport_code").alias("pickup_airport_code"),
    ).alias("pickup_zone")
    dropoff_zone = spark.read.table("dim_dropoff_zone").select(
        "dropoff_zone_key",
        "dropoff_location_id",
        f.col("airport_code").alias("dropoff_airport_code"),
    ).alias("dropoff_zone")
    payment = spark.read.table("dim_payment_type").select(
        "payment_type_key",
        "payment_type_code",
    ).alias("payment")
    rate = spark.read.table("dim_rate_code").select(
        "rate_code_key",
        "rate_code_id",
        f.col("airport_code").alias("rate_airport_code"),
    ).alias("rate")
    vendor = spark.read.table("dim_vendor").select(
        "vendor_key",
        "vendor_id",
    ).alias("vendor")

    joined = (
        trips.join(
            pickup_date,
            f.col("trip._pickup_date") == f.col("pickup_date.pickup_full_date"),
            "left",
        )
        .join(
            dropoff_date,
            f.col("trip._dropoff_date") == f.col("dropoff_date.dropoff_full_date"),
            "left",
        )
        .join(
            pickup_time,
            f.col("trip._pickup_hour") == f.col("pickup_time.pickup_time_key"),
            "left",
        )
        .join(
            dropoff_time,
            f.col("trip._dropoff_hour") == f.col("dropoff_time.dropoff_time_key"),
            "left",
        )
        .join(
            pickup_zone,
            f.col("trip.pickup_location_id")
            == f.col("pickup_zone.pickup_location_id"),
            "left",
        )
        .join(
            dropoff_zone,
            f.col("trip.dropoff_location_id")
            == f.col("dropoff_zone.dropoff_location_id"),
            "left",
        )
        .join(
            payment,
            f.col("trip.payment_type_id") == f.col("payment.payment_type_code"),
            "left",
        )
        .join(
            rate,
            f.col("trip.rate_code_id") == f.col("rate.rate_code_id"),
            "left",
        )
        .join(
            vendor,
            f.col("trip.vendor_id") == f.col("vendor.vendor_id"),
            "left",
        )
    )

    pickup_date_key = f.coalesce(
        f.col("pickup_date.pickup_date_key"), f.lit(0)
    ).cast("int")
    dropoff_date_key = f.coalesce(
        f.col("dropoff_date.dropoff_date_key"), f.lit(0)
    ).cast("int")
    pickup_time_key = f.coalesce(
        f.col("pickup_time.pickup_time_key"), f.lit(-1)
    ).cast("int")
    dropoff_time_key = f.coalesce(
        f.col("dropoff_time.dropoff_time_key"), f.lit(-1)
    ).cast("int")
    pickup_zone_key = f.coalesce(
        f.col("pickup_zone.pickup_zone_key"), f.lit(0)
    ).cast("int")
    dropoff_zone_key = f.coalesce(
        f.col("dropoff_zone.dropoff_zone_key"), f.lit(0)
    ).cast("int")
    payment_type_key = f.coalesce(
        f.col("payment.payment_type_key"), f.lit(-1)
    ).cast("int")
    rate_code_key = f.coalesce(f.col("rate.rate_code_key"), f.lit(-1)).cast(
        "int"
    )
    vendor_key = f.coalesce(f.col("vendor.vendor_key"), f.lit(-1)).cast("int")
    airport_trip_code = f.coalesce(
        f.col("pickup_zone.pickup_airport_code"),
        f.col("dropoff_zone.dropoff_airport_code"),
        f.col("rate.rate_airport_code"),
    )
    reported_card_tip_is_supported = (
        (f.col("trip.payment_type_id") == f.lit(1))
        & f.col("trip.is_reported_tip_metric_eligible")
    )

    descriptive_quality_columns = (
        "passenger_data_status",
        "distance_data_status",
        "financial_record_type",
        "dq_warning_reasons",
        "dq_warning_count",
        "has_dq_warnings",
        "eligibility_reasons",
        "eligibility_restriction_count",
        "eligibility_status",
        "quality_rule_version",
        "silver_refresh_timestamp",
        "eligibility_refresh_timestamp",
    )

    diagnostic_flag_columns = (
        "is_flex_fare",
        "is_zero_distance",
        "is_passenger_count_missing",
        "is_zero_passenger_count",
        "is_financially_unreconciled",
        "is_negative_total_amount",
        "has_negative_financial_component",
        "is_cross_year_trip",
        "is_reversal_or_adjustment",
        "requires_data_review",
        "requires_gold_date_extension",
    )

    metric_eligibility_columns = (
        "is_trip_volume_metric_eligible",
        "is_passenger_metric_eligible",
        "is_distance_metric_eligible",
        "is_duration_metric_eligible",
        "is_efficiency_metric_eligible",
        "is_efficiency_kpi_eligible",
        "is_recorded_amount_metric_eligible",
        "is_financial_breakdown_metric_eligible",
        "is_reported_tip_metric_eligible",
        "is_rate_code_metric_eligible",
        "is_vendor_metric_eligible",
        "is_payment_type_metric_eligible",
        "is_route_metric_eligible",
    )

    ml_eligibility_columns = (
        "is_standard_operational_trip_eligible",
        "is_ml_distance_feature_eligible",
        "is_ml_passenger_feature_eligible",
        "is_ml_financial_feature_eligible",
        "is_ml_categorical_feature_eligible",
        "is_ml_standard_trip_eligible",
    )

    return joined.select(
        f.col("trip.source_record_hash").alias("trip_key"),
        f.col("trip.source_record_hash"),
        pickup_date_key.alias("pickup_date_key"),
        dropoff_date_key.alias("dropoff_date_key"),
        pickup_time_key.alias("pickup_time_key"),
        dropoff_time_key.alias("dropoff_time_key"),
        pickup_zone_key.alias("pickup_zone_key"),
        dropoff_zone_key.alias("dropoff_zone_key"),
        payment_type_key.alias("payment_type_key"),
        rate_code_key.alias("rate_code_key"),
        vendor_key.alias("vendor_key"),
        f.concat_ws(
            ":", pickup_zone_key.cast("string"), dropoff_zone_key.cast("string")
        ).alias("route_key"),
        f.col("trip.pickup_datetime"),
        f.col("trip.dropoff_datetime"),
        f.col("trip.passenger_count"),
        f.col("trip.trip_distance_miles"),
        f.col("trip.trip_duration_minutes"),
        f.col("trip.fare_amount"),
        f.col("trip.extra_amount"),
        f.col("trip.mta_tax_amount"),
        f.col("trip.tip_amount"),
        f.col("trip.tolls_amount"),
        f.col("trip.improvement_surcharge_amount"),
        f.col("trip.total_amount"),
        f.col("trip.congestion_surcharge_amount"),
        f.col("trip.airport_fee_amount"),
        f.col("trip.cbd_congestion_fee_amount"),
        f.col("trip.financial_component_amount"),
        f.col("trip.financial_reconciliation_difference"),
        f.lit(1).cast("long").alias("trip_count"),
        f.col("trip.source_year"),
        f.col("trip.source_month"),
        f.coalesce(
            f.upper(f.trim(f.col("trip.store_and_fwd_flag"))) == f.lit("Y"),
            f.lit(False),
        ).alias("is_store_and_forward"),
        airport_trip_code.isNotNull().alias("is_airport_trip"),
        airport_trip_code.alias("airport_trip_code"),
        f.coalesce(
            f.when(
                reported_card_tip_is_supported,
                f.col("trip.tip_amount"),
            ),
            f.lit(0).cast("decimal(18,2)"),
        ).alias("reported_card_tip_amount"),
        f.coalesce(
            reported_card_tip_is_supported
            & (f.col("trip.tip_amount") > f.lit(0)),
            f.lit(False),
        ).alias("has_reported_electronic_tip"),
        *[
            f.col(f"trip.{column_name}")
            for column_name in descriptive_quality_columns
        ],
        *[
            f.col(f"trip.{column_name}")
            for column_name in diagnostic_flag_columns
        ],
        *[
            f.col(f"trip.{column_name}")
            for column_name in metric_eligibility_columns
        ],
        *[
            f.col(f"trip.{column_name}")
            for column_name in ml_eligibility_columns
        ],
        f.current_timestamp().alias("gold_refresh_timestamp"),
    )
