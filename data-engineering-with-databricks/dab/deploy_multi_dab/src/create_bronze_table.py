# Databricks notebook source
dbutils.widgets.text('catalog_name', 'test_catalog')
dbutils.widgets.text('target', 'development')

# COMMAND ----------

# DBTITLE 1,variables
target = dbutils.widgets.get('target')
catalog = dbutils.widgets.get('catalog_name')
schema = dbutils.widgets.get('schema_name')
source = dbutils.widgets.get('source')
row_limit = dbutils.widgets.get('row_limit')

set_default_catalog = spark.sql(f"USE CATALOG {catalog}")
print(f'using {catalog}.')
print(f'deploying {target} pipeline.')

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE FUNCTION IF NOT EXISTS pii_mask(pii STRING)
# MAGIC   RETURN CASE WHEN is_account_group_member('supervisor') THEN pii ELSE '*****' END;

# COMMAND ----------

spark.sql(
    f'''
    CREATE OR REPLACE TABLE {catalog}.{schema}.health_bronze AS
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
    LIMIT {row_limit};
    '''
)


# COMMAND ----------

spark.sql(f"""
ALTER TABLE {catalog}.{schema}.health_bronze
  ALTER COLUMN Income SET MASK pii_mask;
""")
