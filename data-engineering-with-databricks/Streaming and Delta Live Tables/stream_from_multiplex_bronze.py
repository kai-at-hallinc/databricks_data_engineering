import dlt
import pyspark.sql.functions as F

@dlt.table(
    table_properties = {
        "quality": "bronze"}
)
def bpm_bronze():
    bpm_schema = "device_id LONG, time TIMESTAMP, heartrate DOUBLE"
    return (
        dlt.read_stream("test_catalog.bronze.bronze")
            .filter("topic = 'bpm'")
            .select(F.from_json(
                F.col("value").cast("string"), bpm_schema
            ).alias("v"))
            .select("v.*")
    )