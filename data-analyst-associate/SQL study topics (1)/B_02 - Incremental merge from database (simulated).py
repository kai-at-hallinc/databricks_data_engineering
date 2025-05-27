# Databricks notebook source
# =======================================================================================
# Chapter B_02 - Incremental Merge from Database (Simulated)
# =======================================================================================
#
# OBJECTIVE
# ---------------------------------------------------------------------------------------
# This notebook simulates a typical data engineering scenario where new records
# from a database are incrementally merged into an existing Delta table using the
# `MERGE INTO` operation. The notebook demonstrates both real and simulated data flows
# to show how upserts (update + insert) can be applied efficiently using Delta Lake.
#
# While no real database is connected, all key steps are covered using simulated data.
#
# ENVIRONMENT SETUP
# ---------------------------------------------------------------------------------------
# - A user-isolated schema is created dynamically in Unity Catalog
# - A personal S3 and DBFS folder is initialized per user
# - All tables and file paths are scoped to the current user to avoid conflicts
#
# WORKFLOW OVERVIEW
# ---------------------------------------------------------------------------------------
# 1. Simulate a JDBC database connection (no real connection is made)
# 2. Switch between real DB query and simulated DataFrame using `isDataBase` flag
# 3. Simulate a set of changes that would be pulled from the source system
# 4. Use `MERGE INTO` to apply the changes to the Delta table `employee_delta`:
#     - If `ssn` matches → update `salary`
#     - If not matched   → insert full row
# 5. Use Time Travel to inspect how the table looked before and after the merge
# 6. Display full Delta Lake version history using `DESCRIBE HISTORY`
# 7. Compare multiple versions of the table row by row
#
# SCHEMA USED
# ---------------------------------------------------------------------------------------
# All records (both in Delta and incoming data) follow this schema:
# - ssn          (STRING)   → unique identifier per employee
# - first_name   (STRING)
# - last_name    (STRING)
# - salary       (INT)
# - entry_date   (DATE)
#
# SPECIAL NOTES
# ---------------------------------------------------------------------------------------
# - The entire flow is user-safe and works in shared environments
# - Delta Lake enables tracking of historical versions through Time Travel
# - MERGE operations are idempotent and scale well for daily incremental loads
# - This notebook serves as a template for integrating CDC-style updates
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

# =======================================================================================
# SIMULATED CONFIGURATION FOR EXTERNAL DATABASE CONNECTION (JDBC)
# =======================================================================================
# In a real scenario, this block would be used to configure the connection
# to an external relational database (e.g., SQL Server, PostgreSQL, MySQL, etc.)
# via JDBC.
#
# You would use this configuration to load data into a DataFrame using:
#     spark.read.format("jdbc").options(...).load()
#
# Since this is a simulated environment, no connection is actually made.
# =======================================================================================

# Define JDBC connection parameters (example: SQL Server)
jdbc_url = "jdbc:sqlserver://my-server.database.windows.net:1433;database=myDB"
db_table = "dbo.employees"
db_user = "my_user"
db_password = "my_password"

# Show configuration (without revealing credentials)
print("Database connection parameters configured (simulation only).")


# COMMAND ----------

# =======================================================================================
# DATA SOURCE CONTROL (DATABASE OR SIMULATION)
# =======================================================================================
# The variable `isDataBase` controls whether data is loaded from an external database (1)
# or generated manually as a simulated DataFrame (0).
# =======================================================================================

isDataBase = 0  # Change to 1 to load from DB, 0 to simulate data manually

if isDataBase == 1:
    # ===================================================================================
    # EXTRACT DATA FROM EXTERNAL DATABASE (JDBC)
    # ===================================================================================
    # Executes a SQL query over the external database and stores the result
    # in a Spark DataFrame (`q`). Requires a valid JDBC connection.
    # ===================================================================================

    query = """
    SELECT ssn, first_name, last_name, salary, entry_date
    FROM empleados
    WHERE last_name IN ('Reina', 'Rivera', 'Cruz')
    """

    q = spark.read.format("jdbc") \
        .option("url", jdbc_url) \
        .option("query", query) \
        .option("user", db_user) \
        .option("password", db_password) \
        .load()

    print("Data loaded from external database.")

else:
    # ===================================================================================
    # MANUAL VERSION: SIMULATED DATAFRAME FOR TESTING
    # ===================================================================================
    # This branch is used when no database connection is available.
    # A static dataset is created to simulate changes for MERGE.
    # ===================================================================================

    from datetime import date

    q = spark.createDataFrame([
        ("AAAAA", "Amy",      "Reina",   6500, date(2025, 1, 1)),  # Salary update
        ("CCCCC", "Carla",    "Rivera",  7200, date(2025, 3, 1)),  # Salary update
        ("DDDDD", "Danielle", "Cruz",    5000, date(2025, 4, 1))   # New record
    ], ["ssn", "first_name", "last_name", "salary", "entry_date"])

    print("Simulated data for testing:")

# Show the resulting DataFrame
q.show()


# COMMAND ----------

from delta.tables import DeltaTable
from pyspark.sql.functions import col

# =======================================================================================
# MERGE BETWEEN INCOMING DATA AND DELTA TABLE (UPSERT LOGIC)
# =======================================================================================
# This operation merges new records (from q) into the existing Delta table.
# If a record with the same `ssn` exists, it updates the salary.
# If it does not exist, it inserts the new record.
#
# This logic works regardless of whether `q` came from a database or was simulated.
# =======================================================================================

# Get the user schema from widget
schema = dbutils.widgets.get("user_schema")

# Reference to the Delta table
delta_table = DeltaTable.forName(spark, f"{schema}.employee_delta")

# Perform the MERGE operation
(
    delta_table.alias("trgt")
    .merge(
        q.alias("src"),
        "trgt.ssn = src.ssn AND trgt.salary <> src.salary" # only if salary changed
    )
    .whenMatchedUpdate(set={
        "salary": col("src.salary")
    })
    .whenNotMatchedInsert(values={
        "ssn":        col("src.ssn"),
        "first_name": col("src.first_name"),
        "last_name":  col("src.last_name"),
        "salary":     col("src.salary"),
        "entry_date": col("src.entry_date")
    })
    .execute()
)

print("MERGE completed successfully. Delta table has been updated.")


# COMMAND ----------

# =======================================================================================
# VALIDATION OF DATA BEFORE AND AFTER MERGE
# =======================================================================================
# This section retrieves the Delta table content before and after the MERGE operation.
# It uses Delta Lake's Time Travel feature to access version 0 of the table,
# which represents the original state before the MERGE was applied.
# =======================================================================================

# Get schema from widget
schema = dbutils.widgets.get("user_schema")
table = f"{schema}.employee_delta"

# View table contents BEFORE the MERGE (version 0)
print("Delta table contents BEFORE the MERGE (version 1):")
df_antes = spark.sql(f"SELECT * FROM {table} VERSION AS OF 1")
df_antes.show()

# View table contents AFTER the MERGE (latest version)
print("Delta table contents AFTER the MERGE (current version):")
df_despues = spark.sql(f"SELECT * FROM {table}")
df_despues.show()


# COMMAND ----------

# MAGIC %sql
# MAGIC -- =======================================================================================
# MAGIC -- DELTA LAKE VERSION HISTORY
# MAGIC -- =======================================================================================
# MAGIC -- Shows all changes applied to the Delta table over time,
# MAGIC -- including the MERGE we just executed.
# MAGIC -- =======================================================================================
# MAGIC
# MAGIC DESCRIBE HISTORY ${user_schema}.employee_delta;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- =======================================================================================
# MAGIC -- COMPARE TABLE CONTENT BETWEEN VERSIONS (TIME TRAVEL)
# MAGIC -- =======================================================================================
# MAGIC -- This shows how each row looked before and after the MERGE using UNION.
# MAGIC -- The "version" column indicates which snapshot it came from.
# MAGIC -- =======================================================================================
# MAGIC
# MAGIC SELECT 'v0' AS version, * FROM ${user_schema}.employee_delta VERSION AS OF 0
# MAGIC UNION ALL
# MAGIC SELECT 'v1' AS version, * FROM ${user_schema}.employee_delta VERSION AS OF 1
# MAGIC ORDER BY ssn, version;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- REVIEW QUESTIONS -- 
# MAGIC -- The following questions are recommended to reinforce the concepts from this notebook.
# MAGIC
# MAGIC -- 1. What is the purpose of using a MERGE operation in Delta Lake?
# MAGIC -- 2. What happens if a record already exists when performing a MERGE?
# MAGIC -- 3. How does Delta Lake allow inspecting table versions before and after a change?
# MAGIC -- 4. What is "Time Travel" in Delta Lake?
# MAGIC -- 5. How can you simulate a database connection in Databricks for development/testing?
# MAGIC -- 6. How do you refer to a Delta table in PySpark using DeltaTable?
# MAGIC -- 7. What does `whenMatchedUpdate` do inside a MERGE statement?
# MAGIC -- 8. What are the benefits of isolating each user with schema and path configuration?
# MAGIC -- 9. Why is it important to test incremental logic even without a real database?
# MAGIC -- 10. What does `VERSION AS OF` allow you to do?
# MAGIC
# MAGIC -- -----------------------------------------------------------------------
# MAGIC
# MAGIC -- ANSWERS --
# MAGIC
# MAGIC -- 1. To apply upserts—updating existing records and inserting new ones—in one atomic operation.
# MAGIC -- 2. The row is updated with the new values as specified in the `whenMatchedUpdate` clause.
# MAGIC -- 3. By using Time Travel features like `VERSION AS OF` and `DESCRIBE HISTORY`.
# MAGIC -- 4. Time Travel allows querying previous versions of a Delta table using version numbers or timestamps.
# MAGIC -- 5. By creating simulated DataFrames that mimic the structure and behavior of database outputs.
# MAGIC -- 6. Using DeltaTable.forName(spark, "<schema>.<table_name>").
# MAGIC -- 7. It updates matching records based on a condition (e.g., matching `ssn`).
# MAGIC -- 8. To avoid conflicts in shared environments and allow multiple users to run notebooks independently.
# MAGIC -- 9. It ensures that merge logic works as expected and is ready for production use.
# MAGIC -- 10. It enables viewing and comparing historical states of a table for auditing or debugging purposes.
# MAGIC
