# Databricks notebook source
dbutils.widgets.text('catalog_name', 'test_catalog')
dbutils.widgets.text('target', 'development')

# COMMAND ----------

target = dbutils.widgets.get('target')
catalog = dbutils.widgets.get('catalog_name')
schema = dbutils.widgets.get('schema_name')
source = dbutils.widgets.get('source')
row_limit = dbutils.widgets.get('row_limit')

set_default_catalog = spark.sql(f"USE CATALOG {catalog}")
print(f'using {catalog}.')
print(f'deploying {target} pipeline.')

# COMMAND ----------


spark.sql(
    f'''
    CREATE OR REPLACE TABLE {catalog}.{schema}.health_silver AS
    SELECT
        * EXCEPT (file_name, file_modification_time, ingestion_time)
    FROM {catalog}.{schema}.health_bronze;
    '''
)

