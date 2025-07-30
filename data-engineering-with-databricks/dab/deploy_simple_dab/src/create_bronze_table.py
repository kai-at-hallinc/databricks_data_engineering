# Databricks notebook source
dbutils.widgets.text('catalog_name', 'test_catalog')
dbutils.widgets.text('target', 'development')

# COMMAND ----------

catalog = dbutils.widgets.get('catalog_name')
target = dbutils.widgets.get('target')
source = '/Volumes/databricks_diabetes_health_indicators_dataset/v01/cdc-diabetes/diabetes_binary_5050_raw.csv'

set_default_catalog = spark.sql(f"USE CATALOG {catalog}")
print(f'using {catalog}.')
print(f'deploying {target} pipeline.')

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE FUNCTION pii_mask(pii STRING)
# MAGIC   RETURN CASE WHEN is_account_group_member('supervisor') THEN pii ELSE '*****' END;

# COMMAND ----------

spark.sql(
    f'''
    CREATE OR REPLACE TABLE {catalog}.default.health_bronze AS
    SELECT
        *,
        _metadata.file_name AS file_name,
        _metadata.file_modification_time AS file_modification_time,
        current_timestamp() AS ingestion_time
    FROM read_files(
        '{source}',
        format => 'csv',
        header => true,
        sep => ','
    )
    LIMIT 7500;
    '''
)


# COMMAND ----------

spark.sql(f"""
ALTER TABLE {catalog}.default.health_bronze
  ALTER COLUMN Income SET MASK pii_mask;
""")
