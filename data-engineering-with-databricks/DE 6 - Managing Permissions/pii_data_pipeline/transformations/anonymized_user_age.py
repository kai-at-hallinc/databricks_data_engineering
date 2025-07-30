import dlt
from pyspark.sql.functions import (
    col,
    broadcast,
    to_date,
    cast,
    from_json,
    from_unixtime,
    floor,
    months_between,
    current_date,
    when
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    LongType
)

# date lookup table
@dlt.table(name="test_catalog.default.date_lookup")
def date_lookup():
    path = "/Volumes/dbacademy/v01/date-lookup"
    return spark.read.format("delta").load(path).select("date", "week_part")


@dlt.table(
    name="test_catalog.default.user_events_raw",
    partition_cols=["topic", "week_part"],
    table_properties={"quality": "bronze"},
)
def user_events_raw():
    path = "/Volumes/dbacademy/v01/bronze"
    # get lookup table
    lookup = dlt.read("date_lookup").alias("d")
    return (
        spark.readStream.format("delta")
        .load(path).drop("date", "week_part").alias("u")
        .join(
            broadcast(lookup),
            to_date((col("u.timestamp") / 1000).cast("timestamp")) == col("d.date"),
            "inner"
        )
    )

@dlt.table(
    name="test_catalog.default.users_bronze",
    table_properties={"quality": "bronze"}
)
def users_bronze():
    user_schema = StructType([
        StructField("address", StructType([
            StructField("city", StringType()),
            StructField("state", StringType()),
            StructField("street_address", StringType()),
            StructField("zip", LongType())
        ])),
        StructField("dob", StringType()),
        StructField("first_name", StringType()),
        StructField("gender", StringType()),
        StructField("last_name", StringType()),
        StructField("sex", StringType()),
        StructField("timestamp", LongType()),
        StructField("update_type", StringType()),
        StructField("user_id", LongType())
    ])
    return(
        dlt.read_stream("user_events_raw")
        .filter("topic = 'user_info'")
        .select(from_json(col("value").cast("string"), user_schema).alias("v"))
        .select("v.*")
        .select(
            col("user_id"),
            col("timestamp").cast("timestamp").alias("updated"),
            to_date("dob", "MM/dd/yyyy").alias("dob"),
            "first_name",
            "last_name",
            "sex",
            "gender",
            "address.*",
            "update_type"
        )
    )

def generate_age_bins(dob):
    age_column = floor(months_between(current_date(), dob) / 12).alias("age")
    return (
        when(age_column < 18, "under 18")
        .when((age_column >= 18) & (age_column < 25), "18-25")
        .when((age_column >= 25) & (age_column < 35), "25-35")
        .when((age_column >= 35) & (age_column < 45), "35-45")
        .when((age_column >= 45) & (age_column < 55), "45-55")
        .when((age_column >= 55) & (age_column < 65), "55-65")
        .when((age_column >= 65) & (age_column < 75), "65-75")
        .when((age_column >= 75) & (age_column < 85), "75-85")
        .when((age_column >= 85) & (age_column < 95), "85-95")
        .when((age_column >= 95) & (age_column <= 100), "95-100")
        .otherwise("incorrect age")
        .alias("age_bin")
    )

@dlt.table(
    name="test_catalog.default.user_age_bins",
    table_properties={"quality": "silver"}
)
def user_age_bins():
    return (
        dlt.read("users_bronze")
        .select(
            "user_id",
            generate_age_bins(col("dob")),
            "gender",
            "city",
            "state"
        )
    )
