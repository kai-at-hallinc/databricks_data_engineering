-- Databricks notebook source
-- MAGIC %python
-- MAGIC from pyspark.sql.functions import (
-- MAGIC     col,
-- MAGIC     broadcast,
-- MAGIC     to_date,
-- MAGIC     cast,
-- MAGIC     from_json,
-- MAGIC     from_unixtime,
-- MAGIC )
-- MAGIC from pyspark.sql.types import StructType, StructField, LongType, DoubleType
-- MAGIC

-- COMMAND ----------

-- DBTITLE 1,users
SELECT * FROM test_catalog.default.registered_users
ORDER BY device_id
LIMIT 5;

-- COMMAND ----------

DESCRIBE EXTENDED test_catalog.default.registered_users;

-- COMMAND ----------

-- DBTITLE 1,hashed users
SELECT * FROM test_catalog.default.user_lookup_hashed
ORDER BY device_id
LIMIT 5;

-- COMMAND ----------

SELECT *
FROM test_catalog.default.registered_user_tokens
LIMIT 5;

-- COMMAND ----------

SELECT
  alt_id AS tokenized_id,
  device_id,
  mac_address,
  registration_timestamp
FROM test_catalog.default.user_lookup_tokenized
LIMIT 5;

-- COMMAND ----------

-- DBTITLE 1,date lookup
SELECT * FROM delta.`/Volumes/dbacademy/v01/date-lookup`
LIMIT 5;

-- COMMAND ----------

-- DBTITLE 1,events
SELECT  CAST(value AS STRING) AS value_str FROM delta.`/Volumes/dbacademy/v01/kafka-30min`
WHERE topic = "bpm"
LIMIT 5;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC _sqldf.printSchema()
-- MAGIC
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(
-- MAGIC     spark.sql("""
-- MAGIC         SELECT CAST(value AS STRING) AS value_str FROM test_catalog.default.users_bronze
-- MAGIC         WHERE topic = 'user_info'
-- MAGIC         """
-- MAGIC     )
-- MAGIC )
