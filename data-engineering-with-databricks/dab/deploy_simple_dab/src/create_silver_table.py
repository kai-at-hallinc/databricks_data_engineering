# Databricks notebook source
dbutils.widgets.text('catalog_name', 'test_catalog')
dbutils.widgets.text('target', 'development')

# COMMAND ----------

catalog = dbutils.widgets.get('catalog_name')
target = dbutils.widgets.get('target')
source = f'{catalog}.default.health_bronze'

set_default_catalog = spark.sql(f"USE CATALOG {catalog}")
print(f'using {catalog}.')
print(f'deploying {target} pipeline.')

# COMMAND ----------


spark.sql(
    f'''
    CREATE OR REPLACE TABLE {catalog}.default.health_silver AS
    SELECT
        * EXCEPT (file_name, file_modification_time, ingestion_time)
    FROM {source}
    '''
)

