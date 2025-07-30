from pyspark.sql.functions import (
    col,
    rand
)

bronze_source = "/Volumes/dbacademy/v01/pii/raw/"
ckpt = "/Volumes/test_catalog/landing/cdf_checkpoint_volume/bronze"

CREATE OR REPLACE TABLE test_catalog.default.silver_users
DEEP CLONE DELTA.`/Volumes/dbacademy/v01/pii/silver/`