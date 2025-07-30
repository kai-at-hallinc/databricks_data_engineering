-- Databricks notebook source
-- MAGIC %python
-- MAGIC from pyspark.sql.functions import (
-- MAGIC     col,
-- MAGIC     rand
-- MAGIC )

-- COMMAND ----------

-- DBTITLE 1,pick schema
USE CATALOG test_catalog;
USE SCHEMA default;

-- COMMAND ----------

-- DBTITLE 1,drop source volume
DROP VOLUME iF EXISTS test_catalog.landing.source_volume;

-- COMMAND ----------

-- DBTITLE 1,create source volume
CREATE VOLUME IF NOT EXISTS test_catalog.landing.cdf_source_volume;

-- COMMAND ----------

-- DBTITLE 1,load data
-- MAGIC %python
-- MAGIC df = spark.read.format("delta").load("/Volumes/dbacademy/v01/pii/raw/")
-- MAGIC df.orderBy(rand()).limit(int(df.count() * 0.50)).write.format("delta").mode("append").save("/Volumes/test_catalog/landing/cdf_source_volume")

-- COMMAND ----------

-- DBTITLE 1,create bronze table
CREATE OR REPLACE TABLE bronze_users_cdf AS
SELECT * FROM DELTA.`/Volumes/test_catalog/landing/cdf_source_volume`;

-- COMMAND ----------

-- DBTITLE 1,drop silver
drop table silver_users_cdf;

-- COMMAND ----------

-- DBTITLE 1,create silver table
CREATE OR REPLACE TABLE test_catalog.default.silver_users_cdf
DEEP CLONE DELTA.`/Volumes/dbacademy/v01/pii/silver/`

-- COMMAND ----------

-- DBTITLE 1,set cdf
ALTER TABLE silver_users_cdf
SET TBLPROPERTIES(
  delta.enableChangeDataFeed = true
);

-- COMMAND ----------

describe extended silver_users_cdf;

-- COMMAND ----------

describe history silver_users_cdf;

-- COMMAND ----------

-- DBTITLE 1,upsert fn
-- MAGIC %python
-- MAGIC def upsert_to_delta(microBatchDF, batchId):
-- MAGIC     microBatchDF.createOrReplaceTempView("updates")
-- MAGIC
-- MAGIC     microBatchDF._jdf.sparkSession().sql("""
-- MAGIC         MERGE INTO silver_users_cdf tgt
-- MAGIC         USING updates src
-- MAGIC         ON tgt.mrn = src.mrn
-- MAGIC         WHEN MATCHED AND
-- MAGIC             tgt.dob <> src.dob OR
-- MAGIC             tgt.sex <> src.sex OR
-- MAGIC             tgt.gender <> src.gender OR
-- MAGIC             tgt.first_name <> src.first_name OR
-- MAGIC             tgt.last_name <> src.last_name OR
-- MAGIC             tgt.street_address <> src.street_address OR
-- MAGIC             tgt.zip <> src.zip OR
-- MAGIC             tgt.city <> src.city OR
-- MAGIC             tgt.state <> src.state OR
-- MAGIC             tgt.updated <> src.updated
-- MAGIC         THEN UPDATE SET *
-- MAGIC         WHEN NOT MATCHED
-- MAGIC         THEN INSERT * 
-- MAGIC         """)
-- MAGIC
-- MAGIC     

-- COMMAND ----------

-- DBTITLE 1,upsert to silver
-- MAGIC %python
-- MAGIC silver_users_stream = (
-- MAGIC     spark.readStream
-- MAGIC     .table("bronze_users_cdf")
-- MAGIC     .writeStream
-- MAGIC     .option("checkpointLocation", "/Volumes/test_catalog/landing/cdf_checkpoint_volume/silver")
-- MAGIC     .foreachBatch(upsert_to_delta)
-- MAGIC     .trigger(once=True)
-- MAGIC     .start()
-- MAGIC )

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC cdf_df = (
-- MAGIC   spark.read
-- MAGIC     .format("delta")
-- MAGIC     .option("readChangeData", True)
-- MAGIC     .option("startingVersion", 2)
-- MAGIC     .table("silver_users_cdf")
-- MAGIC )
-- MAGIC display(cdf_df.limit(5))

-- COMMAND ----------

SELECT * FROM table_changes("silver_users_cdf", 2)
WHERE _change_type IN('delete')
ORDER BY _commit_version


-- COMMAND ----------

-- MAGIC %python
-- MAGIC for stream in spark.streams.active:
-- MAGIC     stream.stop()
