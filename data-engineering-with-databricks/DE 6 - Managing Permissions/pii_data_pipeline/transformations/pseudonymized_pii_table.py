import dlt
from pyspark.sql.functions import sha2, lit, concat, col, expr

@dlt.table(
    name="test_catalog.default.registered_users",
    comment="Contains data of registered users"
)
def registered_users():
    return (
        spark.readStream
            .format("cloudFiles")
            .schema("user_id LONG, device_id LONG, mac_address STRING, registration_timestamp DOUBLE")
            .option("cloudFiles.format", "json")
            .load("/Volumes/dbacademy/v01/user-reg")
    )

salt = "beans"

# hash function
def salted_hash(id):
    return sha2(
        concat(lit(id), lit(salt) ), 256
    )

# method:hashing
@dlt.table(
    name="test_catalog.default.user_lookup_hashed",
    comment="Contains hashed data of registered users"
)
def user_lookup_hashed():
    return(
        dlt.read_stream("registered_users")
            .select(
                salted_hash(col("user_id")).alias("alt_id"),
                "device_id",
                "mac_address",
                "registration_timestamp"
            )
    )

# method:tokenize
@dlt.table(
    name="test_catalog.default.registered_user_tokens",
    comment="Contains token data of registered users"
)
def registered_user_tokens():
    return(
        dlt.read_stream("registered_users")
            .select("user_id")
            .distinct()
            .withColumn("token", expr("uuid()"))
        )

@dlt.table(
    name="test_catalog.default.user_lookup_tokenized",
    comment="Contains token data of registered users"
)
def user_lookup_tokenized():
    return(
        dlt.read_stream("registered_users")
            .join(dlt.read("registered_user_tokens"), "user_id", "left")
            .drop("user_id")
            .withColumnRenamed("token", "alt_id")
    )


