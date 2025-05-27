# Databricks notebook source
# MAGIC %sql
# MAGIC /*
# MAGIC ==============================================================================
# MAGIC DELTA TABLE OPTIMIZATION AND FILE COMPACTION WITH `OPTIMIZE`
# MAGIC ==============================================================================
# MAGIC
# MAGIC Why is `OPTIMIZE` important in Delta Lake?
# MAGIC - Databricks stores Delta tables as Parquet files.
# MAGIC - Every time data is inserted or updated in a Delta table, new small files are created.
# MAGIC - An excess of small files slows down queries and consumes more resources.
# MAGIC - `OPTIMIZE` merges those small files into larger ones to improve performance.
# MAGIC
# MAGIC ==============================================================================
# MAGIC How does `OPTIMIZE` work?
# MAGIC - `OPTIMIZE` scans the files within a Delta table and combines small files into larger ones.
# MAGIC - It does not alter the data itself — only reorganizes physical storage to make reads more efficient.
# MAGIC - It is recommended to run `OPTIMIZE` periodically on large tables to avoid file fragmentation.
# MAGIC
# MAGIC ==============================================================================
# MAGIC Example usage:
# MAGIC
# MAGIC OPTIMIZE table_name;
# MAGIC ============================================================================== 
# MAGIC Verifying the optimization with DESCRIBE HISTORY:
# MAGIC
# MAGIC
# MAGIC DESCRIBE HISTORY table_name;
# MAGIC Allows you to see how many files were removed (numRemovedFiles) and how many were created (numAddedFiles).
# MAGIC
# MAGIC ============================================================================== Recommended use cases:
# MAGIC
# MAGIC Tables with frequent inserts that generate many small files
# MAGIC
# MAGIC Large tables where query performance has degraded over time due to fragmentation
# MAGIC
# MAGIC Tables used in high-volume queries where read speed is critical
# MAGIC
# MAGIC ============================================================================== 
# MAGIC Precautions:
# MAGIC
# MAGIC OPTIMIZE can be computationally expensive — it should be scheduled during off-peak hours.
# MAGIC
# MAGIC It is not recommended for small tables, where the performance gain is minimal.
# MAGIC
# MAGIC It does not replace data partitioning (PARTITION BY), but complements it.
# MAGIC
# MAGIC ============================================================================== 
# MAGIC Conclusion: OPTIMIZE is a key tool for improving query performance in Databricks. 
# MAGIC It consolidates small files into larger ones, reducing storage fragmentation. It should be used selectively, evaluating the trade-offs in each use case.
# MAGIC
# MAGIC */

# COMMAND ----------


# ---------------------------------------------------------------------------------------
# USER-SPECIFIC ENVIRONMENT SETUP (Must be run at the beginning of the notebook)
# ---------------------------------------------------------------------------------------
# This setup block prepares an isolated working environment for each user.
# It ensures that all users in a shared Databricks workspace can run the same notebook
# without interfering with each other's data or database objects.
#
# What this block does:
#
# 1. Retrieves the current Databricks user email using `current_user()`.
# 2. Normalizes the username (removes dots and the domain part of the email).
#    For example: 'peter.spider@company.com' becomes 'peter_spider'.
# 3. Constructs a unique schema name (e.g., `sql_review_peter_spider`) and a unique S3 path:
#    s3://databricks-hatchworks-main-bucket/external_data/peter_spider/sql_review
# 4. Creates the schema in the Unity Catalog if it doesn't already exist.
# 5. Ensures that the corresponding S3 folder exists (or ignores the error if it already does).
# 6. Registers two widgets: 
#       - `user_schema`: used in SQL cells to dynamically refer to the user's schema
#       - `user_path`: used to store files in the user's personal S3 directory
#
# NOTE:
# This block must be executed before any other operation in the notebook.
# If skipped, the notebook will fail due to missing context variables.
# ---------------------------------------------------------------------------------------


# === setup_user_env ===
# dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
# === setup_user_env ===

# Get user and normalize name
username = spark.sql("SELECT current_user()").collect()[0][0]
username_prefix = username.split("@")[0].replace(".", "_")

# Define schema name and S3 path
schema_name = f"sql_review_{username_prefix}" # Catalog -> sandbox -> sql_review_{username_prefix}
folder_path = f"s3://databricks-hatchworks-main-bucket/external_data/{username_prefix}/sql_review" # Amazon S3 -> Buckets -> databricks-hatchworks-main-bucket -> external_data/

# Create schema if it doesn't exist
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")

# Create folder in S3 (ignore errors if it already exists)
try:
    dbutils.fs.mkdirs(folder_path)
except Exception as e:
    print(f"Note: Could not create folder {folder_path}. It may already exist.")

# Re-register widgets (even if they already exist)
dbutils.widgets.removeAll()
dbutils.widgets.text("user_schema", schema_name)
dbutils.widgets.text("user_path", folder_path)

# Optional: print confirmation
print(f"user_schema: {schema_name}")
print(f"user_path:   {folder_path}")

# COMMAND ----------

# =======================================================================================
# CREATE DELTA TABLE WITH SMALL FILES (FOR OPTIMIZATION DEMO)
# =======================================================================================
# This simulates fragmentation by inserting small batches of employee records.
# The Delta table will end up with many small files to demonstrate OPTIMIZE later.
# =======================================================================================

from pyspark.sql.types import StringType, IntegerType, StructType, StructField

# Get user schema from widget
schema = dbutils.widgets.get("user_schema")
table_name = f"{schema}.employees_optimize"

# Define schema
employee_schema = StructType([
    StructField("ssn", StringType(), False),
    StructField("name", StringType(), False),
    StructField("salary", IntegerType(), False)
])

# Define data in 5 small batches (3 records each)
data_batch_1 = [("AAAAA", "Carlos", 5000), ("BBBBB", "Amy", 5500), ("CCCCC", "Luis", 5800)]
data_batch_2 = [("DDDDD", "Pedro", 5200), ("EEEEE", "Elena", 6100), ("FFFFF", "John", 6300)]
data_batch_3 = [("GGGGG", "Sofia", 6000), ("HHHHH", "David", 5700), ("IIIII", "Laura", 5400)]
data_batch_4 = [("JJJJJ", "Brian", 5900), ("KKKKK", "Lucia", 6200), ("LLLLL", "Steve", 6100)]
data_batch_5 = [("MMMMM", "Paula", 5600), ("NNNNN", "Marco", 5800), ("OOOOO", "Emily", 6000)]

# Convert to DataFrames
df1 = spark.createDataFrame(data_batch_1, schema=employee_schema)
df2 = spark.createDataFrame(data_batch_2, schema=employee_schema)
df3 = spark.createDataFrame(data_batch_3, schema=employee_schema)
df4 = spark.createDataFrame(data_batch_4, schema=employee_schema)
df5 = spark.createDataFrame(data_batch_5, schema=employee_schema)

# Drop table if it exists
spark.sql(f"DROP TABLE IF EXISTS {table_name}")

# Write each batch separately (simulating fragmented small files)
df1.write.format("delta").mode("overwrite").saveAsTable(table_name)
df2.write.format("delta").mode("append").saveAsTable(table_name)
df3.write.format("delta").mode("append").saveAsTable(table_name)
df4.write.format("delta").mode("append").saveAsTable(table_name)
df5.write.format("delta").mode("append").saveAsTable(table_name)

print(f"Delta table {table_name} created with 15 employees in 5 separate transactions.")


# COMMAND ----------

# MAGIC %sql
# MAGIC -- =======================================================================================
# MAGIC -- FETCH DELTA TABLE METADATA FROM UNITY CATALOG SYSTEM DICTIONARY
# MAGIC -- =======================================================================================
# MAGIC -- In Databricks Unity Catalog, we can query `system.information_schema.tables`
# MAGIC -- to retrieve metadata for managed or external tables, including storage location.
# MAGIC -- =======================================================================================
# MAGIC
# MAGIC SELECT 
# MAGIC   table_catalog, 
# MAGIC   table_schema, 
# MAGIC   table_name, 
# MAGIC   storage_path,
# MAGIC   table_type,
# MAGIC   data_source_format,
# MAGIC   created,
# MAGIC   last_altered
# MAGIC FROM system.information_schema.tables
# MAGIC WHERE table_name = 'employees_optimize';
# MAGIC

# COMMAND ----------

from delta.tables import DeltaTable

# Get table reference
schema = dbutils.widgets.get("user_schema")
table_path = f"{schema}.employees_optimize"

# Use the DeltaTable API to get details
delta_table = DeltaTable.forName(spark, table_path)

# Get list of all data files from Delta log
add_files = delta_table.detail().select("location").collect()
print(f"DeltaTable reports {len(add_files)} active data file(s).")


# COMMAND ----------

# MAGIC %sql
# MAGIC -- =======================================================================================
# MAGIC -- VERIFY DELTA TABLE DETAILS BEFORE OPTIMIZATION
# MAGIC -- =======================================================================================
# MAGIC -- `DESCRIBE HISTORY` shows all operations performed on the Delta table,
# MAGIC -- including how many files were added or removed in each write.
# MAGIC -- 
# MAGIC -- Look at `numAddedFiles`, `operation`, and `operationMetrics` columns to
# MAGIC -- confirm the number of small files created prior to running `OPTIMIZE`.
# MAGIC -- =======================================================================================
# MAGIC
# MAGIC DESCRIBE HISTORY ${user_schema}.employees_optimize;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- =======================================================================================
# MAGIC -- APPLY OPTIMIZE TO COMPACT SMALL FILES
# MAGIC -- =======================================================================================
# MAGIC -- The `OPTIMIZE` command merges multiple small files into fewer large ones,
# MAGIC -- which improves query performance and reduces file system overhead.
# MAGIC --
# MAGIC -- After running OPTIMIZE, check `DESCRIBE HISTORY` to confirm:
# MAGIC -- - `numRemovedFiles`: e.g., 5 (original fragments)
# MAGIC -- - `numAddedFiles`:   e.g., 1 (the compacted output)
# MAGIC -- =======================================================================================
# MAGIC
# MAGIC OPTIMIZE ${user_schema}.employees_optimize;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- =======================================================================================
# MAGIC -- VERIFY DELTA TABLE DETAILS AFTER `OPTIMIZE`
# MAGIC -- =======================================================================================
# MAGIC -- Use `DESCRIBE HISTORY` to confirm that the OPTIMIZE operation was executed.
# MAGIC -- The output will include file-level metrics that indicate the optimization results.
# MAGIC --
# MAGIC -- NOTE: The following values are just reference examples based on a previous run.
# MAGIC -- Your actual values may differ depending on cluster configuration and data layout.
# MAGIC --
# MAGIC -- Example:
# MAGIC -- - numRemovedFiles:  5        → 5 small files were removed
# MAGIC -- - numAddedFiles:    1        → 1 large optimized file was created
# MAGIC -- - numRemovedBytes:  4884     → About 4.88 KB of fragmented data removed
# MAGIC -- - numAddedBytes:    1065     → Final optimized file is ~1.06 KB
# MAGIC -- =======================================================================================
# MAGIC
# MAGIC DESCRIBE HISTORY ${user_schema}.employees_optimize;
# MAGIC
