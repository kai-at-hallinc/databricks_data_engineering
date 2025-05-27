# Databricks notebook source
# =======================================================================================
# PySpark & Delta Merge - Chapter 2 of the Delta Lake Workshop
# =======================================================================================
#
# OBJECTIVE
# ---------------------------------------------------------------------------------------
# This notebook demonstrates how to work with PySpark DataFrames and Delta Lake tables
# using a typical use case: performing incremental data loads via MERGE operations.
# It includes examples of transformations, conditional logic, and schema management.
#
# USER-ISOLATED ENVIRONMENT
# ---------------------------------------------------------------------------------------
# The first cell of this notebook (setup_user_env) is critical for supporting
# multi-user execution. It:
# - Retrieves the current user's Databricks login
# - Generates a user-specific schema (e.g., sql_review_jane_doe)
# - Defines a user-specific S3 folder path for data storage
# - Ensures both schema and folder exist
# - Registers widgets for dynamic access: `user_schema`, `user_path`
#
# Every database object created in this notebook is scoped to the user.
# This guarantees isolation, avoids collisions, and enables repeatable runs.
#
# CONTENT OVERVIEW
# ---------------------------------------------------------------------------------------
#
# 1. **DataFrame Creation**  
#    - A DataFrame is created using a hardcoded list of employees.
#    - The schema includes SSN, first name, last name, salary, and entry date.
#
# 2. **Derived Columns**  
#    - Columns like `annual_salary` and `salary_band` are created using PySpark functions.
#    - Demonstrates use of `withColumn`, expressions, and conditional logic (`when`, `otherwise`).
#
# 3. **Data Cleanup**  
#    - Demonstrates how to drop a column using `df.drop()`.
#
# 4. **New Incoming Data**  
#    - A new DataFrame simulates incoming data with:
#      - Salary updates for existing employees
#      - A brand new employee
#
# 5. **MERGE Operation**  
#    - Loads the existing Delta table using `DeltaTable.forName`.
#    - Performs a `MERGE` that:
#      - Updates existing rows by SSN
#      - Inserts new rows if no match is found
#
# 6. **Validation**  
#    - The final SQL cell queries the table to confirm the result of the merge.
#
# TECHNICAL NOTES
# ---------------------------------------------------------------------------------------
# - The Delta table `employee_delta` must exist before running the merge.
#   It can be created in a previous notebook (e.g., A_01).
# - The `user_schema` widget allows SQL cells to remain dynamic and reusable.
# - Date fields use the `datetime.date` Python object for correctness.
# - This notebook uses pure PySpark, with no external dependencies.
#
# SAFE TO RE-RUN
# ---------------------------------------------------------------------------------------
# - All operations are idempotent.
# - Running this notebook multiple times will not break the schema or the Delta table.
# - It supports collaborative, safe, multi-user learning and experimentation.
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
# Basic PySpark Example in Databricks
# --------------------------------------------------------------
# This cell demonstrates how to create and display a simple Spark DataFrame
# manually using hardcoded data. It is intended for demonstration purposes 
# and to validate that PySpark is working as expected in this notebook.
# --------------------------------------------------------------

# Import the necessary PySpark class for handling Spark operations
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType
from datetime import date

# Create a SparkSession, which is the entry point to using Spark
# In Databricks, this is usually pre-created as 'spark', 
# but we include it here for educational purposes or portability
spark = SparkSession.builder.appName("TestPySpark").getOrCreate()

# Define a small dataset using a list of tuples
# Each tuple represents a row in the DataFrame
data = [
    ("AAAAA", "Amy",   "Reina",  5000, date(2025, 1, 1)),
    ("BBBBB", "Bruce", "Trillo", 6000, date(2025, 2, 1)),
    ("CCCCC", "Carla", "Rivera", 7000, date(2025, 3, 1))
]

# Define column names for the DataFrame
columns = ["SSN", "first_name", "last_name", "salary", "entry_date"]

# Create the DataFrame from the data and columns
df = spark.createDataFrame(data, columns)

# Show the content of the DataFrame in tabular format
df.show()


# COMMAND ----------

# --------------------------------------------------------------
# Add a new column: annual salary
# --------------------------------------------------------------
# This step demonstrates how to transform a DataFrame by adding a derived column.
# We calculate the annual salary by multiplying the monthly salary by 12.
# The new column is named 'annual_salary'.
df = df.withColumn("annual_salary", df["salary"] * 12)

# Display the updated DataFrame including the new column
df.show()


# COMMAND ----------

# --------------------------------------------------------------
# Add a new column: salary_band
# --------------------------------------------------------------
# In this example, we use conditional logic to classify each employee
# based on their monthly salary into categories (Low, Medium, High).
# This is a common pattern in PySpark using 'when' and 'otherwise'.

from pyspark.sql.functions import when

# Apply conditional logic to define salary bands
df = df.withColumn(
    "salary_band",
    when(df["salary"] < 5500, "Low")
    .when(df["salary"] < 6500, "Medium")
    .otherwise("High")
)

# Display the updated DataFrame with the new classification column
df.show()


# COMMAND ----------

# --------------------------------------------------------------
# Drop the 'annual_salary' column from the DataFrame
# --------------------------------------------------------------
# This demonstrates how to remove a column from a Spark DataFrame.
# The 'drop' method creates a new DataFrame without the specified column.
df = df.drop("annual_salary")

# Display the resulting DataFrame to confirm the column was removed
display(df)


# COMMAND ----------

from pyspark.sql.functions import col
from datetime import date

# --------------------------------------------------------------
# New employee data to be inserted or used for updates
# --------------------------------------------------------------
# This dataset includes:
# - Salary updates for existing employees
# - A new employee not present in the original table

new_data = [
    ("AAAAA", "Amy", "Reina",   6500, date(2025, 1, 1)),  # Amy got a raise from 5000 to 6500
    ("BBBBB", "Bruce", "Trillo", 7700, date(2025, 2, 1)), # Bruce got a raise from 6000 to 7700
    ("DDDDD", "Danielle", "Cruz", 5000, date(2025, 4, 1)) # Danielle is a new employee
]

# Define column names
columns = ["SSN", "first_name", "last_name", "salary", "entry_date"]

# Create a new DataFrame with the updated data
df_new = spark.createDataFrame(new_data, columns)

# Display the new DataFrame
df_new.show()


# COMMAND ----------

from delta.tables import DeltaTable
from pyspark.sql.functions import col

# --------------------------------------------------------------
# Merge new employee data into an existing Delta table
# --------------------------------------------------------------
# This operation performs an "upsert":
# - If the SSN already exists, the employee's information is updated
# - If the SSN does not exist, the employee is inserted

# Get schema from widget
schema = dbutils.widgets.get("user_schema")

# Load the existing Delta table
delta_table = DeltaTable.forName(spark, f"{schema}.employee_delta")

# Perform the MERGE operation
(
    delta_table.alias("target")
    .merge(
        df_new.alias("source"),
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

print("Incremental load completed.")


# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from ${user_schema}.employee_delta

# COMMAND ----------

# MAGIC %sql
# MAGIC -- REVIEW QUESTIONS -- 
# MAGIC -- The following questions are recommended to reinforce the concepts from this notebook.
# MAGIC
# MAGIC -- 1. What is a Delta Lake MERGE operation used for?
# MAGIC -- 2. How does Delta Lake identify matching rows during a MERGE?
# MAGIC -- 3. What PySpark function is used to add a new column to a DataFrame?
# MAGIC -- 4. How do you apply conditional logic to create a derived column in PySpark?
# MAGIC -- 5. How can you remove a column from a DataFrame?
# MAGIC -- 6. What is the purpose of using DeltaTable.forName() in PySpark?
# MAGIC -- 7. What are the roles of 'whenMatchedUpdate' and 'whenNotMatchedInsert' in a MERGE?
# MAGIC -- 8. What makes the operations in this notebook idempotent?
# MAGIC -- 9. What are widgets used for in Databricks notebooks?
# MAGIC -- 10. Why is it important to isolate users via schema and path in shared environments?
# MAGIC
# MAGIC -- -----------------------------------------------------------------------
# MAGIC
# MAGIC -- ANSWERS --
# MAGIC
# MAGIC -- 1. To perform an "upsert" (update existing records or insert new ones) based on a match condition.
# MAGIC -- 2. By evaluating a condition in the ON clause, typically comparing primary keys or unique fields.
# MAGIC -- 3. withColumn()
# MAGIC -- 4. Using the `when(...).otherwise(...)` expression from `pyspark.sql.functions`.
# MAGIC -- 5. Using the `drop()` method on the DataFrame.
# MAGIC -- 6. To reference and manipulate an existing Delta table programmatically.
# MAGIC -- 7. They define how to handle matched rows (update) and unmatched rows (insert) during the merge.
# MAGIC -- 8. Because running them multiple times with the same data will not produce duplicates or errors.
# MAGIC -- 9. Widgets allow notebooks to accept dynamic input like schema names or file paths, making code reusable.
# MAGIC -- 10. To avoid conflicts and allow multiple users to run the same notebook without affecting each other.
# MAGIC
