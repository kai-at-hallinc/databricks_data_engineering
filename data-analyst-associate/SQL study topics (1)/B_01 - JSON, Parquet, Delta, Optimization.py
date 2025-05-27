# Databricks notebook source
# =======================================================================================
# Chapter B_01 - Working with JSON, Parquet, and Delta Formats in Databricks
# =======================================================================================
#
# OBJECTIVE
# ---------------------------------------------------------------------------------------
# This notebook explores how to work with three different data formats commonly used
# in modern data pipelines: JSON, Parquet, and Delta Lake. It simulates a scenario
# where a base dataset is read from a CSV file and then converted to these formats,
# stored in user-specific folders, and loaded into Delta tables for querying and analysis.
#
# ENVIRONMENT SETUP
# ---------------------------------------------------------------------------------------
# Before anything else, the notebook initializes a user-isolated environment:
# - Each user gets a unique schema in Unity Catalog (e.g., sql_review_john_doe)
# - A dedicated S3 folder for shared input/output files
# - A private DBFS folder under `/tmp/<user>/sql_review/` for temporary file storage
#
# This setup ensures that users can run the same notebook concurrently without
# interfering with each other’s data or tables.
#
# WORKFLOW OVERVIEW
# ---------------------------------------------------------------------------------------
# 1. Clean and create a working folder in DBFS for storing file outputs (JSON, Parquet)
# 2. Read the employee_base.csv file from the user's S3 path
# 3. Normalize data types (especially DATE and INT fields)
# 4. Save the dataset as:
#    - JSON file
#    - Parquet file
# 5. Read both JSON and Parquet files from DBFS
# 6. Save each one as a Delta table in the user's schema:
#    - employee_json
#    - employee_parquet
# 7. Compare contents of both Delta tables using UNION
# 8. Review the column schemas and Delta Lake history for each table
# 9. List the physical files stored for each format in DBFS
#
# ADVANCED OPTIMIZATION (DELTA LAKE)
# ---------------------------------------------------------------------------------------
# 10. Run `OPTIMIZE` on both Delta tables to compact small files and improve performance
# 11. Apply `ZORDER BY` on key columns (e.g., SSN) to accelerate filtered queries
# 12. Use `VACUUM` to clean up obsolete files and free up storage space
#
# These operations ensure long-term efficiency and better query performance,
# especially in environments with frequent updates or large data volumes.
#
# SPECIAL NOTES
# ---------------------------------------------------------------------------------------
# - Spark writes JSON and Parquet as directories containing multiple part files.
# - Delta Lake allows full transactional support, time travel, and schema tracking.
# - ZORDER is not a replacement for partitioning, but works well for highly-filtered columns.
# - VACUUM by default keeps files for 7 days to support time travel.
#   Use `RETAIN 0 HOURS` with caution—it deletes files immediately and disables time travel.
#
# - All schema and path variables are dynamically generated per user for safe multi-user operation.
#
# =======================================================================================


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

# ---------------------------------------------------------------------------------------
# Prepare a DBFS folder under /tmp for storing JSON, Parquet, and Delta files
# ---------------------------------------------------------------------------------------
# This cell creates a working folder in DBFS for the current user at:
# dbfs:/tmp/<username>/sql_review/
#
# NOTE: Even though the user-specific environment setup has already been run,
# we reconstruct the username_prefix here to ensure consistency and avoid re-parsing widgets.
# This folder is separate from the S3-based storage and is meant for local file experiments.
# ---------------------------------------------------------------------------------------

# Rebuild the username prefix (same logic as setup)
username = spark.sql("SELECT current_user()").collect()[0][0]
username_prefix = username.split("@")[0].replace(".", "_")

# Define DBFS path
dbfs_folder_path = f"dbfs:/tmp/{username_prefix}/sql_review/"

# Remove folder if it exists
try:
    dbutils.fs.rm(dbfs_folder_path, True)
    print(f"Folder {dbfs_folder_path} removed.")
except:
    print(f"Folder {dbfs_folder_path} could not be removed (it may not exist).")

# Recreate empty folder
dbutils.fs.mkdirs(dbfs_folder_path)
print(f"Folder {dbfs_folder_path} created.")

# Define the widget
dbutils.widgets.text("user_dbfs_path", dbfs_folder_path)



# COMMAND ----------

from pyspark.sql.functions import col, to_date

# ---------------------------------------------------------------------------------------
# Read the base employee CSV from user-specific S3 path
# and save it in JSON and Parquet formats in DBFS
# ---------------------------------------------------------------------------------------

# Get S3 path and DBFS path from widgets
s3_folder = dbutils.widgets.get("user_path")
dbfs_folder = dbutils.widgets.get("user_dbfs_path")

# Define the full path to the CSV file in S3
csv_path = f"{s3_folder}/employee_base.csv"

# Load the CSV into a DataFrame
df_csv = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load(csv_path)

# Cast entry_date to DATE and salary to INT to avoid downstream type mismatches
df_csv = df_csv \
    .withColumn("entry_date", to_date(col("entry_date"))) \
    .withColumn("salary", col("salary").cast("int"))

# Display the content of the CSV
print("Data read from S3 (employee_base.csv):")
df_csv.show()

# Define output paths for JSON and Parquet in DBFS
json_path = f"{dbfs_folder}/employee.json"
parquet_path = f"{dbfs_folder}/employee.parquet"

# Write the DataFrame in JSON and Parquet formats
df_csv.write.format("json").mode("overwrite").save(json_path)
df_csv.write.format("parquet").mode("overwrite").save(parquet_path)

print("JSON and Parquet files saved in user-specific DBFS folder.")


# COMMAND ----------

# ---------------------------------------------------------------------------------------
# List the contents of the user's DBFS working folder
# ---------------------------------------------------------------------------------------

# Get the DBFS path from widget
dbfs_folder = dbutils.widgets.get("user_dbfs_path")

# List and display files and folders under the user's DBFS path
display(dbutils.fs.ls(dbfs_folder))


# COMMAND ----------

# ---------------------------------------------------------------------------------------
# Read and display the JSON and Parquet files saved in DBFS
# ---------------------------------------------------------------------------------------

dbfs_folder = dbutils.widgets.get("user_dbfs_path")

# Paths to the folders written by Spark
json_path = f"{dbfs_folder}/employee.json"
parquet_path = f"{dbfs_folder}/employee.parquet"

# Read JSON and Parquet files
df_json = spark.read.format("json").load(json_path)
df_parquet = spark.read.format("parquet").load(parquet_path)

print("Data from JSON file:")
df_json.show()

print("Data from Parquet file:")
df_parquet.show()


# COMMAND ----------

# ---------------------------------------------------------------------------------------
# Save JSON and Parquet data as Delta tables using user-specific schema
# ---------------------------------------------------------------------------------------

# Write mode explanation:
# "overwrite" → Replaces the table completely (YES, drops and recreates it)
# "append"    → Appends new data without deleting existing rows (NO, just adds)
# "error"     → Fails if the table already exists (NO, throws error)
# "ignore"    → Skips writing if the table already exists (NO, silently skips)
# ---------------------------------------------------------------------------------------

# Get user schema from widget
schema = dbutils.widgets.get("user_schema")

# Save JSON data as a Delta table
df_json.write.format("delta").mode("overwrite").saveAsTable(f"{schema}.employee_json")

# Save Parquet data as a Delta table
df_parquet.write.format("delta").mode("overwrite").saveAsTable(f"{schema}.employee_parquet")

print(f"Delta tables created successfully: {schema}.employee_json and {schema}.employee_parquet")


# COMMAND ----------

# MAGIC %sql
# MAGIC -- ---------------------------------------------------------------------------------------
# MAGIC -- Compare contents of both Delta tables with column type normalization
# MAGIC -- ---------------------------------------------------------------------------------------
# MAGIC
# MAGIC SELECT 
# MAGIC   SSN,
# MAGIC   'pq' AS tipo,
# MAGIC   first_name,
# MAGIC   last_name,
# MAGIC   salary,
# MAGIC   entry_date
# MAGIC   -- CAST(entry_date AS DATE) AS entry_date
# MAGIC FROM ${user_schema}.employee_parquet
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 
# MAGIC   SSN,
# MAGIC   'js' AS tipo,
# MAGIC   first_name,
# MAGIC   last_name,
# MAGIC   salary,
# MAGIC   entry_date
# MAGIC   -- CAST(entry_date AS DATE) AS entry_date
# MAGIC FROM ${user_schema}.employee_json
# MAGIC
# MAGIC ORDER BY SSN, tipo;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ---------------------------------------------------------------------------------------
# MAGIC -- Show the schema (column types) of the employee_parquet Delta table
# MAGIC -- ---------------------------------------------------------------------------------------
# MAGIC DESCRIBE TABLE ${user_schema}.employee_parquet;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ---------------------------------------------------------------------------------------
# MAGIC -- Show the schema (column types) of the employee_json Delta table
# MAGIC -- ---------------------------------------------------------------------------------------
# MAGIC DESCRIBE TABLE ${user_schema}.employee_json;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ---------------------------------------------------------------------------------------
# MAGIC -- View the Delta Lake transaction history for employee_parquet
# MAGIC -- ---------------------------------------------------------------------------------------
# MAGIC DESCRIBE HISTORY ${user_schema}.employee_parquet;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ---------------------------------------------------------------------------------------
# MAGIC -- View the Delta Lake transaction history for employee_json
# MAGIC -- ---------------------------------------------------------------------------------------
# MAGIC DESCRIBE HISTORY ${user_schema}.employee_json;
# MAGIC

# COMMAND ----------

# ---------------------------------------------------------------------------------------
# List physical Delta files stored for employee_parquet
# ---------------------------------------------------------------------------------------
dbfs_folder = dbutils.widgets.get("user_dbfs_path")
parquet_path = f"{dbfs_folder}/employee.parquet"

display(dbutils.fs.ls(parquet_path))


# COMMAND ----------

# ---------------------------------------------------------------------------------------
# List physical Delta files stored for employee_json
# ---------------------------------------------------------------------------------------
json_path = f"{dbfs_folder}/employee.json"

display(dbutils.fs.ls(json_path))


# COMMAND ----------

'''
+---------+---------------------------------+------------------------------------+---------------------------------------+-----------------------------------------------------------+
| Format  | Source Path                     | DBFS Location                      | Delta Table Name                      | Notes                                                     |
+---------+---------------------------------+------------------------------------+---------------------------------------+-----------------------------------------------------------+
| CSV     | ${user_path}/employee_base.csv  | (not stored in DBFS)               | (not directly used for table)         | Used as source to create JSON & Parquet                   |
| JSON    | Written from df_csv             | ${user_dbfs_path}/employee.json    | ${user_schema}.employee_json          | entry_date may be string; needs casting to DATE           |
| Parquet | Written from df_csv             | ${user_dbfs_path}/employee.parquet | ${user_schema}.employee_parquet       | Preserves original types more accurately                  |
| Delta   | Created from JSON & Parquet     | (Delta-managed storage)            | See employee_json and employee_parquet| Supports SQL, time travel, history, and schema enforcement|
+----------+---------------------------------------------+-----------------------+---------------------------------------------+-----------------------------------------------------+
'''

# COMMAND ----------

# MAGIC %sql
# MAGIC -- =======================================================================================
# MAGIC -- DELTA TABLE OPTIMIZATION
# MAGIC -- =======================================================================================
# MAGIC -- The `OPTIMIZE` command is used to consolidate many small files into larger ones.
# MAGIC -- In Delta Lake, each transaction (insert/update/delete) creates new files, which can
# MAGIC -- lead to fragmentation and degrade performance over time.
# MAGIC --
# MAGIC -- Benefits of using `OPTIMIZE`:
# MAGIC -- - Reduces file count in storage
# MAGIC -- - Improves query performance
# MAGIC -- - Decreases Spark execution overhead
# MAGIC -- - Highly recommended for frequently updated or loaded tables
# MAGIC --
# MAGIC -- NOTE:
# MAGIC -- This operation can be resource-intensive. Consider scheduling during off-peak hours.
# MAGIC -- After optimization, queries will likely run faster and more efficiently.
# MAGIC -- =======================================================================================
# MAGIC
# MAGIC -- Compact files in the Delta table created from JSON
# MAGIC OPTIMIZE ${user_schema}.employee_json;
# MAGIC
# MAGIC -- Compact files in the Delta table created from Parquet
# MAGIC OPTIMIZE ${user_schema}.employee_parquet;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- =======================================================================================
# MAGIC -- DELTA TABLE OPTIMIZATION WITH ZORDER
# MAGIC -- =======================================================================================
# MAGIC -- ZORDER BY improves query performance in Delta Lake when filtering data using
# MAGIC -- WHERE clauses or JOINs on specific columns.
# MAGIC --
# MAGIC -- Benefits:
# MAGIC -- - Organizes data layout based on frequently filtered columns
# MAGIC -- - Accelerates searches without requiring table partitioning
# MAGIC -- - Recommended for columns commonly used in filters or joins
# MAGIC --
# MAGIC -- NOTE: ZORDER must be used after OPTIMIZE (not before).
# MAGIC -- =======================================================================================
# MAGIC
# MAGIC -- Optimize and apply ZORDER to employee_json based on 'ssn'
# MAGIC OPTIMIZE ${user_schema}.employee_json ZORDER BY (ssn);
# MAGIC
# MAGIC -- Optimize and apply ZORDER to employee_parquet based on 'ssn'
# MAGIC OPTIMIZE ${user_schema}.employee_parquet ZORDER BY (ssn);
# MAGIC
# MAGIC -- =======================================================================================
# MAGIC -- MULTI-COLUMN ZORDER (Optional)
# MAGIC -- =======================================================================================
# MAGIC -- You can ZORDER by more than one column:
# MAGIC --     Example: OPTIMIZE table ZORDER BY (col1, col2);
# MAGIC -- - Data is ordered first by col1, then by col2 within that
# MAGIC -- - Useful when queries involve filters on multiple fields
# MAGIC --
# MAGIC -- Tip: Only use 1–3 columns to avoid unnecessary overhead.
# MAGIC --
# MAGIC -- Example (commented out):
# MAGIC -- -- OPTIMIZE ${user_schema}.employee_json ZORDER BY (ssn, salary);
# MAGIC -- -- OPTIMIZE ${user_schema}.employee_parquet ZORDER BY (ssn, salary);
# MAGIC -- =======================================================================================
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- =======================================================================================
# MAGIC -- DELTA TABLE OPTIMIZATION WITH VACUUM
# MAGIC -- =======================================================================================
# MAGIC -- The VACUUM command removes old, unused files from Delta tables,
# MAGIC -- freeing up storage space and keeping file systems efficient.
# MAGIC --
# MAGIC -- By default, Delta Lake retains deleted files for 7 days to support
# MAGIC -- Time Travel and recovery from accidental deletions.
# MAGIC --
# MAGIC -- Benefits:
# MAGIC -- - Reduces storage usage by removing obsolete files
# MAGIC -- - Maintains read performance by cleaning up file clutter
# MAGIC -- - Prevents long-term buildup of unused data
# MAGIC --
# MAGIC -- NOTE:
# MAGIC -- Setting a retention period of 0 HOURS will remove all old files immediately,
# MAGIC -- but this will disable Time Travel to previous versions.
# MAGIC -- Use it only when you're sure you won’t need to access the past state.
# MAGIC -- =======================================================================================
# MAGIC
# MAGIC -- Run VACUUM using the default 7-day retention
# MAGIC VACUUM ${user_schema}.employee_json;
# MAGIC VACUUM ${user_schema}.employee_parquet;
# MAGIC
# MAGIC -- Optional (Use with caution):
# MAGIC -- -- VACUUM ${user_schema}.employee_json RETAIN 0 HOURS;
# MAGIC -- -- VACUUM ${user_schema}.employee_parquet RETAIN 0 HOURS;
# MAGIC -- WARNING: Using RETAIN 0 HOURS disables Time Travel permanently.
# MAGIC

# COMMAND ----------

# ---------------------------------------------------------------------------------------
# Clean up the user's working folder in DBFS (reset to initial state)
# ---------------------------------------------------------------------------------------

# Get the dynamic DBFS path from widget
folder_path = dbutils.widgets.get("user_dbfs_path")

# Delete the entire folder and its contents (recursive = True)
dbutils.fs.rm(folder_path, True)

print(f"Folder {folder_path} deleted successfully.")


# COMMAND ----------

# MAGIC %sql
# MAGIC -- REVIEW QUESTIONS -- 
# MAGIC -- The following questions are recommended to reinforce the concepts from this notebook.
# MAGIC
# MAGIC -- 1. What are the key differences between JSON, Parquet, and Delta file formats?
# MAGIC -- 2. What does the OPTIMIZE command do in Delta Lake?
# MAGIC -- 3. What is ZORDER and when should it be used?
# MAGIC -- 4. What happens when you run VACUUM on a Delta table?
# MAGIC -- 5. Why is the RETAIN 0 HOURS option for VACUUM dangerous?
# MAGIC -- 6. How can you improve query performance on non-partitioned Delta tables?
# MAGIC -- 7. How can you compare schemas between two Delta tables?
# MAGIC -- 8. Why might a JSON file require casting of certain columns (e.g., dates)?
# MAGIC -- 9. What command can you use to view the Delta transaction history?
# MAGIC -- 10. Where are physical Delta files stored, and how can you list them?
# MAGIC
# MAGIC -- -----------------------------------------------------------------------
# MAGIC
# MAGIC -- ANSWERS --
# MAGIC
# MAGIC -- 1. JSON is verbose and untyped, Parquet is binary and columnar, Delta adds ACID transactions, schema evolution, and time travel on top of Parquet.
# MAGIC -- 2. OPTIMIZE compacts small files in a Delta table into larger ones to improve performance.
# MAGIC -- 3. ZORDER organizes data files to speed up queries that filter on specific columns; used after OPTIMIZE.
# MAGIC -- 4. VACUUM removes obsolete files no longer referenced by the Delta transaction log.
# MAGIC -- 5. RETAIN 0 HOURS deletes all unreferenced files immediately and disables the ability to perform time travel.
# MAGIC -- 6. Use ZORDER BY on frequently-filtered columns to improve data skipping and scan efficiency.
# MAGIC -- 7. Use DESCRIBE TABLE <table_name> to inspect schema, or compare using SELECT and UNION across tables.
# MAGIC -- 8. Because Spark may infer entry_date as STRING from JSON and requires casting to DATE explicitly.
# MAGIC -- 9. DESCRIBE HISTORY <table_name>
# MAGIC -- 10. Physical Delta files can be viewed with dbutils.fs.ls() pointing to the storage location of the table.
# MAGIC
