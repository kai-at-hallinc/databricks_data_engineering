# Databricks notebook source
/*  
==============================================================================
DATA VERSIONING AND HISTORY IN DELTA LAKE
==============================================================================

-- What is data versioning in Delta Lake?
-- Delta Lake automatically stores a complete history of changes to a table.
-- Every write operation (INSERT, UPDATE, DELETE, MERGE) creates a new version.
-- You can query previous versions and access historical data easily.

==============================================================================
-- How to view the history of a table?
DESCRIBE HISTORY table_name;
-- This shows all historical versions with metadata such as:
--   version: The version number.
--   timestamp: Date and time of the change.
--   operation: Type of operation (INSERT, UPDATE, DELETE, MERGE).
--   userName: The user who performed the operation.
--   operationMetrics: Number of records affected.

==============================================================================
-- How to query data from a previous version?
-- Delta Lake supports time travel using either version or timestamp.

-- 1. By version number
SELECT * FROM table_name VERSION AS OF 3;

-- 2. By timestamp
SELECT * FROM table_name TIMESTAMP AS OF '2024-01-01 12:00:00';
-- Useful for auditing, debugging, or recovering past data.

==============================================================================
-- How to restore a previous version?
-- To revert a table back to a previous state:
RESTORE TABLE table_name TO VERSION AS OF 3;
-- This will bring the table back to the specified version.

==============================================================================
-- Caution
-- Delta Lake does not delete old versions automatically.
-- Use VACUUM to clean up unused history and old files.
-- By default, version history is retained for 30 days 
-- (controlled by `delta.logRetentionDuration`).

==============================================================================
-- Conclusion
-- Delta Lake keeps a full audit trail of table changes.
-- You can query historical data without affecting the current state.
-- Useful for troubleshooting, debugging, and compliance audits.
==============================================================================
*/


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

# MAGIC %sql
# MAGIC -- =======================================================================================
# MAGIC -- CREATE DELTA TABLE FOR VERSIONING DEMONSTRATION
# MAGIC -- =======================================================================================
# MAGIC -- This creates a Delta table named `employees_versioning` to test data history and time travel.
# MAGIC -- The first INSERT creates version 0 of the table.
# MAGIC -- =======================================================================================
# MAGIC
# MAGIC CREATE OR REPLACE TABLE ${user_schema}.employees_versioning (
# MAGIC     ssn STRING,
# MAGIC     first_name STRING,
# MAGIC     salary INT,
# MAGIC     department STRING
# MAGIC ) USING DELTA;
# MAGIC
# MAGIC -- =======================================================================================
# MAGIC -- INSERT INITIAL RECORDS (VERSION 0)
# MAGIC -- =======================================================================================
# MAGIC INSERT INTO ${user_schema}.employees_versioning VALUES
# MAGIC ('AAAAA', 'Amy',    5000, 'Sales'),
# MAGIC ('BBBBB', 'Bruce',  6000, 'IT'),
# MAGIC ('CCCCC', 'Carla',  7000, 'HR');
# MAGIC
# MAGIC -- =======================================================================================
# MAGIC -- DISPLAY INITIAL DATA (VERSION 0)
# MAGIC -- =======================================================================================
# MAGIC SELECT * FROM ${user_schema}.employees_versioning;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- =======================================================================================
# MAGIC -- APPLY CHANGES TO GENERATE MULTIPLE TABLE VERSIONS
# MAGIC -- =======================================================================================
# MAGIC -- Insert new records → Creates new version
# MAGIC INSERT INTO ${user_schema}.employees_versioning VALUES
# MAGIC ('DDDDD', 'Danielle', 5500, 'Sales'),
# MAGIC ('EEEEE', 'Ethan',    6200, 'IT');
# MAGIC
# MAGIC -- Update salary of an existing employee → Creates new version
# MAGIC UPDATE ${user_schema}.employees_versioning 
# MAGIC SET salary = 7500 
# MAGIC WHERE ssn = 'CCCCC';
# MAGIC
# MAGIC -- Delete a record → Creates new version
# MAGIC DELETE FROM ${user_schema}.employees_versioning 
# MAGIC WHERE ssn = 'AAAAA';
# MAGIC
# MAGIC -- =======================================================================================
# MAGIC -- VIEW TABLE VERSION HISTORY
# MAGIC -- =======================================================================================
# MAGIC DESCRIBE HISTORY ${user_schema}.employees_versioning;
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- View the current state (latest version)
# MAGIC SELECT 'current' AS version, * 
# MAGIC FROM ${user_schema}.employees_versioning;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- View data from version 0 (before initial INSERTs)
# MAGIC SELECT 'v0' AS version, * 
# MAGIC FROM ${user_schema}.employees_versioning VERSION AS OF 0 ORDER BY SSN;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- View data from version 1 (after initial INSERTs)
# MAGIC SELECT 'v1' AS version, * 
# MAGIC FROM ${user_schema}.employees_versioning VERSION AS OF 1 ORDER BY SSN;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- View data from version 2 (after new inserts)
# MAGIC SELECT 'v2' AS version, * 
# MAGIC FROM ${user_schema}.employees_versioning VERSION AS OF 2 ORDER BY SSN;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- View data from version 3 (after update a row)
# MAGIC SELECT 'v3' AS version, * 
# MAGIC FROM ${user_schema}.employees_versioning VERSION AS OF 3 ORDER BY SSN;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- View data from version 4 (after update a row)
# MAGIC SELECT 'v4' AS version, * 
# MAGIC FROM ${user_schema}.employees_versioning VERSION AS OF 4 ORDER BY SSN;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- View data from version 4 (after delete a row)
# MAGIC SELECT 'v5' AS version, * 
# MAGIC FROM ${user_schema}.employees_versioning VERSION AS OF 5 ORDER BY SSN;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- =======================================================================================
# MAGIC -- VIEW THE FULL CHANGE HISTORY OF A DELTA TABLE
# MAGIC -- =======================================================================================
# MAGIC -- `DESCRIBE HISTORY` displays the complete audit trail of operations performed on the table.
# MAGIC -- This includes metadata such as:
# MAGIC --   - version: Auto-incremented version number after each change
# MAGIC --   - timestamp: When the change occurred (UTC)
# MAGIC --   - userName: Who performed the change
# MAGIC --   - operation: Type of action (CREATE, INSERT, UPDATE, DELETE, MERGE, OPTIMIZE, etc.)
# MAGIC --   - operationMetrics: Statistics such as rows affected or files rewritten
# MAGIC --
# MAGIC -- This command is essential for tracking data lineage, debugging, and enabling time travel.
# MAGIC -- =======================================================================================
# MAGIC
# MAGIC DESCRIBE HISTORY ${user_schema}.employees_versioning;
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- =======================================================================================
# MAGIC -- QUERY DATA FROM A SPECIFIC POINT IN TIME (TIME TRAVEL BY TIMESTAMP)
# MAGIC -- =======================================================================================
# MAGIC -- Delta Lake also allows querying historical data using an exact timestamp.
# MAGIC -- The format must be: 'yyyy-MM-dd HH:mm:ss' (UTC timezone).
# MAGIC -- =======================================================================================
# MAGIC
# MAGIC SELECT *
# MAGIC FROM ${user_schema}.employees_versioning
# MAGIC TIMESTAMP AS OF '2025-03-25T14:28:50.000+00:00'; -- <-- Use a timestamp in UTC from previous results 
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- =======================================================================================
# MAGIC -- COMPARE DATA BETWEEN TWO DELTA TABLE VERSIONS
# MAGIC -- =======================================================================================
# MAGIC -- This query pulls records from version 2 and version 1 of the Delta table.
# MAGIC -- It adds a `version` column to identify the source and uses `UNION ALL` to combine them.
# MAGIC -- This is useful for auditing changes and visually identifying what was updated.
# MAGIC -- =======================================================================================
# MAGIC
# MAGIC SELECT '2' AS vers, * 
# MAGIC FROM ${user_schema}.employees_versioning VERSION AS OF 2
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT '1' AS vers, * 
# MAGIC FROM ${user_schema}.employees_versioning VERSION AS OF 1
# MAGIC
# MAGIC ORDER BY ssn, vers;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- REVIEW QUESTIONS --
# MAGIC -- The following questions are recommended to reinforce the concepts from this notebook.
# MAGIC
# MAGIC -- 1. What is Time Travel in Delta Lake, and why is it useful?
# MAGIC -- 2. What SQL command allows you to view the version history of a Delta table?
# MAGIC -- 3. How do you query a Delta table using a specific version?
# MAGIC -- 4. How do you query a Delta table using a specific timestamp?
# MAGIC -- 5. What kind of operations create a new version in Delta Lake?
# MAGIC -- 6. How can you restore a table to a previous state using versioning?
# MAGIC -- 7. What metadata can you find in the output of `DESCRIBE HISTORY`?
# MAGIC -- 8. Does Delta automatically delete old versions of data?
# MAGIC -- 9. How can Time Travel help with auditing and debugging?
# MAGIC -- 10. What is the default retention period for Delta log history?
# MAGIC
# MAGIC -- -----------------------------------------------------------------------
# MAGIC
# MAGIC -- ANSWERS --
# MAGIC
# MAGIC -- 1. Time Travel is the ability to query or restore previous versions of a Delta table using version numbers or timestamps. It's useful for auditing, debugging, and recovery.
# MAGIC -- 2. DESCRIBE HISTORY <table_name>;
# MAGIC -- 3. SELECT * FROM <table_name> VERSION AS OF <version_number>;
# MAGIC -- 4. SELECT * FROM <table_name> TIMESTAMP AS OF 'yyyy-MM-dd HH:mm:ss';
# MAGIC -- 5. Any write operation (INSERT, UPDATE, DELETE, MERGE) creates a new version.
# MAGIC -- 6. RESTORE TABLE <table_name> TO VERSION AS OF <version_number>;
# MAGIC -- 7. Version, timestamp, userName, operation, operationMetrics (e.g., rowsUpdated, rowsInserted).
# MAGIC -- 8. No. Old versions are retained until cleaned up by VACUUM based on the retention policy.
# MAGIC -- 9. It allows comparing historical states, identifying data changes, and undoing mistakes.
# MAGIC -- 10. 30 days by default (controlled by `delta.logRetentionDuration`).
# MAGIC

# COMMAND ----------


