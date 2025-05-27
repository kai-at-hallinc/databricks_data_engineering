# Databricks notebook source
# =======================================================================================
# Delta Lake Workshop - Chapter 3: Ingesting CSV Data and Performing MERGE Operations
# =======================================================================================
#
# OBJECTIVE
# ---------------------------------------------------------------------------------------
# This notebook simulates a simple data pipeline scenario using:
# - CSV files as the data source
# - Delta Lake as the destination table format
# - MERGE operations to handle inserts and updates efficiently
#
# This is the third notebook in the Delta Lake workshop and builds upon concepts 
# introduced earlier. The scenario models a real-world incremental data load:
# importing a base dataset and then applying changes from a second file.
#
# ENVIRONMENT SETUP
# ---------------------------------------------------------------------------------------
# The notebook begins by creating a user-specific environment:
# - A schema named after the user (e.g., sql_review_john_doe)
# - A personal folder in S3 for each user's CSV files
# - Two widgets: `user_schema` and `user_path` for dynamic references
#
# CONTENT OVERVIEW
# ---------------------------------------------------------------------------------------
# 1. Drop and create an empty Delta table (`employee_delta`)
#
# 2. Generate and store two CSV files in the user's S3 folder:
#    - `employee_base.csv`: the initial data load
#    - `employee_changes.csv`: salary updates + new employees
#
# 3. Load both CSV files as Spark DataFrames
#
# 4. Insert the base data into the Delta table and view it
#
# 5. Perform a Delta `MERGE` to apply updates/inserts from `employee_changes.csv`
#
# 6. Query the table again to confirm the merge result
#
# NOTES
# ---------------------------------------------------------------------------------------
# - Users can manually upload their own CSV files to test different merge scenarios.
# - The CSV files are written as single files with `.coalesce(1)` and stored with exact names.
# - The notebook is fully isolated per user and safe to re-run.
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

# --------------------------------------------------------------
# Drop and create an empty Delta table using user-specific schema
# --------------------------------------------------------------

# Get schema from widget
schema = dbutils.widgets.get("user_schema")
table_name = f"{schema}.employee_delta"

# Drop the table if it exists
spark.sql(f"DROP TABLE IF EXISTS {table_name}")
print(f"Table {table_name} dropped (if it existed).")

# Create a new empty Delta table
spark.sql(f"""
    CREATE TABLE {table_name} (
        SSN STRING,
        first_name STRING,
        last_name STRING,
        salary INT,
        entry_date DATE
    ) USING DELTA
""")
print(f"Empty Delta table {table_name} created.")


# COMMAND ----------

# ---------------------------------------------------------------------------------------
# Create a CSV file (employee_base.csv) in the user's personal S3 folder
# ---------------------------------------------------------------------------------------
# This cell prepares a small sample employee dataset and saves it as a single CSV file
# named "employee_base.csv" inside the user's folder on S3:
# s3://databricks-hatchworks-main-bucket/external_data/<username>/sql_review/
#
# What this cell does:
# 1. Creates a DataFrame with sample employee data
# 2. Writes the data to a temporary folder in CSV format (Spark always writes to folders)
# 3. Locates the single .csv file inside that temporary folder
# 4. Moves and renames the file to the final path as "employee_base.csv"
# 5. Deletes the temporary folder to keep the workspace clean
#
# NOTE:
# This file can also be created manually outside Databricks (e.g., in Excel or Notepad)
# and uploaded by the student to the corresponding S3 path, replacing this generated file.
# ---------------------------------------------------------------------------------------

import os
import uuid

# Step 1: Define data
data = [
    ("AAAAA", "Amy",   "Reina",  5000, date(2025, 1, 1)),
    ("BBBBB", "Bruce", "Trillo", 6000, date(2025, 2, 1)),
    ("CCCCC", "Carla", "Rivera", 7000, date(2025, 3, 1))
]
columns = ["SSN", "first_name", "last_name", "salary", "entry_date"]

df = spark.createDataFrame(data, schema=columns)

# Step 2: Get user path from widget
base_path = dbutils.widgets.get("user_path")
temp_path = os.path.join(base_path, f"_tmp_csv_{uuid.uuid4().hex}")
final_path = os.path.join(base_path, "employee_base.csv")

# Step 3: Write to a temporary folder
df.coalesce(1).write.mode("overwrite").option("header", True).csv(temp_path)

# Step 4: Find the actual .csv file inside the temp folder
csv_file = [f.path for f in dbutils.fs.ls(temp_path) if f.path.endswith(".csv")][0]

# Step 5: Move and rename the file
dbutils.fs.mv(csv_file, final_path)

# Step 6: Clean up the temporary folder
dbutils.fs.rm(temp_path, recurse=True)

print(f"CSV saved as: {final_path}")


# COMMAND ----------

# ---------------------------------------------------------------------------------------
# Create a CSV file (employee_changes.csv) with updated/new employee data
# ---------------------------------------------------------------------------------------
# This cell creates a CSV file with employee updates and inserts.
# It is saved in the user's personal S3 folder:
# s3://databricks-hatchworks-main-bucket/external_data/<username>/sql_review/
#
# What this cell does:
# 1. Defines a small dataset representing employee changes:
#    - Existing employees with updated salaries
#    - A new employee not present in the base table
# 2. Creates a Spark DataFrame from this dataset
# 3. Writes it to a temporary folder as a CSV file
# 4. Extracts the actual .csv file and renames it to "employee_changes.csv"
# 5. Deletes the temporary folder to keep the storage clean
#
# NOTE:
# Students can manually create or modify this CSV and upload it directly
# to their corresponding S3 folder to simulate different merge scenarios.
# ---------------------------------------------------------------------------------------

from datetime import date
import os
import uuid

# Step 1: Define the updated/new employee data
new_data = [
    ("AAAAA", "Amy", "Reina",   6500, date(2025, 1, 1)),  # Amy got a raise
    ("BBBBB", "Bruce", "Trillo", 7700, date(2025, 2, 1)), # Bruce got a raise
    ("DDDDD", "Danielle", "Cruz", 5000, date(2025, 4, 1)) # New hire
]

columns = ["SSN", "first_name", "last_name", "salary", "entry_date"]
df_changes = spark.createDataFrame(new_data, schema=columns)

# Step 2: Prepare paths
base_path = dbutils.widgets.get("user_path")
temp_path = os.path.join(base_path, f"_tmp_csv_{uuid.uuid4().hex}")
final_path = os.path.join(base_path, "employee_changes.csv")

# Step 3: Write to temp path
df_changes.coalesce(1).write.mode("overwrite").option("header", True).csv(temp_path)

# Step 4: Locate the CSV file and move it to the final path
csv_file = [f.path for f in dbutils.fs.ls(temp_path) if f.path.endswith(".csv")][0]
dbutils.fs.mv(csv_file, final_path)

# Step 5: Clean up temporary folder
dbutils.fs.rm(temp_path, recurse=True)

print(f"CSV file 'employee_changes.csv' saved to: {final_path}")


# COMMAND ----------

# ---------------------------------------------------------------------------------------
# Load CSV files from the user's personal S3 folder
# ---------------------------------------------------------------------------------------
# This cell loads two CSV files into Spark DataFrames:
# 1. employee_base.csv      -> Contains the original employee records
# 2. employee_changes.csv   -> Contains updated and new employee records
#
# Both files are expected to be located in:
# s3://databricks-hatchworks-main-bucket/external_data/<username>/sql_review/
#
# The path is dynamically built using the 'user_path' widget.
# ---------------------------------------------------------------------------------------

# Get user folder path from widget
base_path = dbutils.widgets.get("user_path")
employee_base_path = f"{base_path}/employee_base.csv"
employee_changes_path = f"{base_path}/employee_changes.csv"

# Load base employee data from CSV
df_base = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(employee_base_path)

# Load changes (updates + inserts) from CSV
df_changes = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(employee_changes_path)

# Show both DataFrames
print("Base employee data:")
df_base.show()

print("Employee changes:")
df_changes.show()


# COMMAND ----------

# ---------------------------------------------------------------------------------------
# Insert the base employee data into the user's Delta table
# and display its contents
# ---------------------------------------------------------------------------------------
# This cell:
# 1. Writes df_base into the user's Delta table 'employee_delta' (overwrite mode)
# 2. Immediately queries and displays the table contents using Spark SQL
# ---------------------------------------------------------------------------------------

# Get schema name from widget
schema = dbutils.widgets.get("user_schema")
table_name = f"{schema}.employee_delta"

# Write the base employee data into the Delta table
df_base.write.format("delta").mode("overwrite").saveAsTable(table_name)

print(f"Base employee data inserted into Delta table: {table_name}")

# Query and display the inserted data
display(spark.sql(f"SELECT * FROM {table_name} ORDER BY SSN"))


# COMMAND ----------

# ---------------------------------------------------------------------------------------
# Perform a MERGE operation to update and insert employee records
# ---------------------------------------------------------------------------------------
# This cell loads the user's Delta table and merges the records from df_changes.
# The merge logic:
# - If a record with the same SSN exists, update all columns
# - If the SSN does not exist, insert the new employee
# ---------------------------------------------------------------------------------------

from delta.tables import DeltaTable
from pyspark.sql.functions import col

# Get schema name from widget
schema = dbutils.widgets.get("user_schema")
table_name = f"{schema}.employee_delta"

# Load the existing Delta table
delta_table = DeltaTable.forName(spark, table_name)

# Execute the MERGE using df_changes as the source
(
    delta_table.alias("target")
    .merge(
        df_changes.alias("source"),
        "target.SSN = source.SSN"
    )
    .whenMatchedUpdate(set={
        "first_name": col("source.first_name"),
        "last_name": col("source.last_name"),
        "salary": col("source.salary"),
        "entry_date": col("source.entry_date")
    })
    .whenNotMatchedInsert(values={
        "SSN": col("source.SSN"),
        "first_name": col("source.first_name"),
        "last_name": col("source.last_name"),
        "salary": col("source.salary"),
        "entry_date": col("source.entry_date")
    })
    .execute()
)

print("MERGE operation completed successfully.")


# COMMAND ----------

# ---------------------------------------------------------------------------------------
# Query the Delta table to review the results after the MERGE
# ---------------------------------------------------------------------------------------
# This cell retrieves and displays the contents of the user's Delta table
# after the merge operation has been completed.
# ---------------------------------------------------------------------------------------

# Get schema and table name
schema = dbutils.widgets.get("user_schema")
table_name = f"{schema}.employee_delta"

# Query and display the table contents
display(spark.sql(f"SELECT * FROM {table_name} ORDER BY SSN"))


# COMMAND ----------

# ---------------------------------------------------------------------------------------
# View the change history of the Delta table
# ---------------------------------------------------------------------------------------
# This cell displays the transaction history of the user's Delta table
# It allows us to track operations like INSERT, MERGE, UPDATE, and their timestamps
# ---------------------------------------------------------------------------------------

# Get schema and table name from widget
schema = dbutils.widgets.get("user_schema")
table_name = f"{schema}.employee_delta"

# Show the Delta transaction history
display(spark.sql(f"DESCRIBE HISTORY {table_name}"))


# COMMAND ----------

# MAGIC %sql
# MAGIC -- REVIEW QUESTIONS -- 
# MAGIC -- The following questions are recommended to reinforce the concepts from this notebook.
# MAGIC
# MAGIC -- 1. What is the purpose of using a MERGE operation in Delta Lake?
# MAGIC -- 2. How can you perform an upsert using PySpark and Delta Lake?
# MAGIC -- 3. What is the difference between `whenMatchedUpdate` and `whenNotMatchedInsert`?
# MAGIC -- 4. What is the role of `.coalesce(1)` when writing CSV files in Spark?
# MAGIC -- 5. How can users ensure they do not overwrite each other's data when using shared notebooks?
# MAGIC -- 6. How do you track changes made to a Delta table over time?
# MAGIC -- 7. What is the effect of running the same notebook multiple times with the same CSV input?
# MAGIC -- 8. How do you dynamically load user-specific paths in Databricks notebooks?
# MAGIC
# MAGIC -- -----------------------------------------------------------------------
# MAGIC
# MAGIC -- ANSWERS --
# MAGIC
# MAGIC -- 1. To synchronize a Delta table with a source dataset by updating existing rows and inserting new ones.
# MAGIC -- 2. By using DeltaTable.forName(...).merge(...).whenMatchedUpdate(...).whenNotMatchedInsert(...).execute().
# MAGIC -- 3. `whenMatchedUpdate` defines the update logic for existing records; `whenNotMatchedInsert` handles new inserts.
# MAGIC -- 4. It forces Spark to write the output as a single file instead of multiple part-files.
# MAGIC -- 5. By creating a user-specific schema and S3 folder using `current_user()` and widgets.
# MAGIC -- 6. Using the `DESCRIBE HISTORY <table>` command to view the transaction log of the Delta table.
# MAGIC -- 7. The operation becomes idempotent if the logic avoids duplication, but repeated inserts may overwrite or append depending on mode.
# MAGIC -- 8. By using widgets like `dbutils.widgets.text(...)` and referencing them with `get("user_path")` or `get("user_schema")`.
# MAGIC
