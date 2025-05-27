-- Databricks notebook source
/*
=======================================================================================
F_02 - FROM TABLE TO ARRAY (OR ARRAY-LIKE STRUCTURES)
=======================================================================================

OBJECTIVE
---------------------------------------------------------------------------------------
This notebook demonstrates how to transform tabular data into array-like structures
using Spark SQL functions such as `collect_list()`, `collect_set()`, `array()`, `struct()`, and `map()`.

These transformations are useful in many real-world scenarios, such as:
- Preparing data for JSON export or APIs
- Building nested or semi-structured datasets
- Feeding data into dashboards or frontends
- Aggregating and compressing data for reporting

WHY THIS MATTERS
---------------------------------------------------------------------------------------
Traditional tabular data works well for relational operations, but modern data workflows
often require more flexibility. Being able to switch between tables and arrays (or structs)
gives you more control over your schema and output format — especially when integrating
with non-SQL systems or consuming nested data.

TRANSFORMATIONS COVERED
---------------------------------------------------------------------------------------

1. collect_list()
   - Groups values (e.g. project names) into an array per group (e.g. per employee)
   - Preserves duplicates

2. collect_set()
   - Like collect_list(), but removes duplicates

3. array()
   - Combines multiple columns into an array per row
   - Useful for building column-level arrays

4. struct()
   - Groups multiple columns into a single nested row (like an object or dictionary)
   - Useful for creating clean nested output structures

5. map()
   - Builds key-value pairs from two columns
   - Ideal for generating dictionaries or tagged attribute sets

TABLES USED
---------------------------------------------------------------------------------------
- `${user_schema}.employee_project_flat`  
  Each row represents one employee assigned to one project.  
  This flat structure is transformed into different array-like formats throughout the notebook.

NOTES
---------------------------------------------------------------------------------------
- All queries are run using Spark SQL
- The results are displayed directly and also shown via Python (optionally) for export
- The notebook is self-contained, reproducible, and ready for experimentation

=======================================================================================
*/


-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC # ---------------------------------------------------------------------------------------
-- MAGIC # USER-SPECIFIC ENVIRONMENT SETUP (Must be run at the beginning of the notebook)
-- MAGIC # ---------------------------------------------------------------------------------------
-- MAGIC # This setup block prepares an isolated working environment for each user.
-- MAGIC # It ensures that all users in a shared Databricks workspace can run the same notebook
-- MAGIC # without interfering with each other's data or database objects.
-- MAGIC #
-- MAGIC # What this block does:
-- MAGIC #
-- MAGIC # 1. Retrieves the current Databricks user email using `current_user()`.
-- MAGIC # 2. Normalizes the username (removes dots and the domain part of the email).
-- MAGIC #    For example: 'peter.spider@company.com' becomes 'peter_spider'.
-- MAGIC # 3. Constructs a unique schema name (e.g., `sql_review_peter_spider`) and a unique S3 path:
-- MAGIC #    s3://databricks-hatchworks-main-bucket/external_data/peter_spider/sql_review
-- MAGIC # 4. Creates the schema in the Unity Catalog if it doesn't already exist.
-- MAGIC # 5. Ensures that the corresponding S3 folder exists (or ignores the error if it already does).
-- MAGIC # 6. Registers two widgets: 
-- MAGIC #       - `user_schema`: used in SQL cells to dynamically refer to the user's schema
-- MAGIC #       - `user_path`: used to store files in the user's personal S3 directory
-- MAGIC #
-- MAGIC # NOTE:
-- MAGIC # This block must be executed before any other operation in the notebook.
-- MAGIC # If skipped, the notebook will fail due to missing context variables.
-- MAGIC # ---------------------------------------------------------------------------------------
-- MAGIC
-- MAGIC
-- MAGIC # === setup_user_env ===
-- MAGIC # dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
-- MAGIC # === setup_user_env ===
-- MAGIC
-- MAGIC # Get user and normalize name
-- MAGIC username = spark.sql("SELECT current_user()").collect()[0][0]
-- MAGIC username_prefix = username.split("@")[0].replace(".", "_")
-- MAGIC
-- MAGIC # Define schema name and S3 path
-- MAGIC schema_name = f"sql_review_{username_prefix}" # Catalog -> sandbox -> sql_review_{username_prefix}
-- MAGIC folder_path = f"s3://databricks-hatchworks-main-bucket/external_data/{username_prefix}/sql_review" # Amazon S3 -> Buckets -> databricks-hatchworks-main-bucket -> external_data/
-- MAGIC
-- MAGIC # Create schema if it doesn't exist
-- MAGIC spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")
-- MAGIC
-- MAGIC # Create folder in S3 (ignore errors if it already exists)
-- MAGIC try:
-- MAGIC     dbutils.fs.mkdirs(folder_path)
-- MAGIC except Exception as e:
-- MAGIC     print(f"Note: Could not create folder {folder_path}. It may already exist.")
-- MAGIC
-- MAGIC # Re-register widgets (even if they already exist)
-- MAGIC dbutils.widgets.removeAll()
-- MAGIC dbutils.widgets.text("user_schema", schema_name)
-- MAGIC dbutils.widgets.text("user_path", folder_path)
-- MAGIC
-- MAGIC # Optional: print confirmation
-- MAGIC print(f"user_schema: {schema_name}")
-- MAGIC print(f"user_path:   {folder_path}")
-- MAGIC
-- MAGIC

-- COMMAND ----------

-- =======================================================================================
-- Base table: One row per employee-project assignment
-- =======================================================================================
-- Simple structure: each row represents one project for one employee
-- This table will be used to demonstrate how to group or combine rows into arrays.
-- =======================================================================================

CREATE OR REPLACE TABLE ${user_schema}.employee_project_flat (
  SSN STRING,
  project_name STRING
) USING DELTA;

INSERT INTO ${user_schema}.employee_project_flat VALUES
  ('AAAAA', 'Website Redesign'),
  ('AAAAA', 'Sales Optimization'),
  ('BBBBB', 'Cloud Migration'),
  ('CCCCC', 'Security Audit'),
  ('CCCCC', 'Recruiting Revamp'),
  ('CCCCC', 'Policy Update'), -- NON UNIQUE
('CCCCC', 'Policy Update'); -- NON UNIQUE


-- COMMAND ----------

-- =======================================================================================
-- collect_list(): Group projects into an array per employee
-- =======================================================================================
-- This function collects all project names associated with each employee (SSN)
-- and returns them as an array.
--
-- Use case: When you want to present data in a denormalized format,
-- such as sending a JSON to a frontend, feeding a report, or storing history.
-- Instead of one row per project, you get one row per employee
-- with an array of their projects. (See employe CCCCC)
-- =======================================================================================

SELECT 
  SSN,
  collect_list(project_name) AS projects
FROM ${user_schema}.employee_project_flat
GROUP BY SSN;


-- COMMAND ----------

-- =======================================================================================
-- collect_set(): Group unique projects into an array per employee
-- =======================================================================================
-- This is similar to collect_list(), but it removes duplicate project names.
--
-- Use case: When you're preparing data for reporting, dashboards, or APIs 
-- and you only care about the distinct values — no need to repeat the same project.
-- This is great when your source data might contain duplicates due to joins or errors. (See employe CCCCC)
-- =======================================================================================

SELECT 
  SSN,
  collect_set(project_name) AS unique_projects
FROM ${user_schema}.employee_project_flat
GROUP BY SSN;


-- COMMAND ----------

-- =======================================================================================
-- array(): Combine multiple columns into a single array per row
-- =======================================================================================
-- This function creates an array from multiple columns in the same row.
--
-- Use case: When you want to package several values together into a single column
-- per row — for example, exporting to JSON, feeding a nested structure, or 
-- simplifying output before sending to another system.
--
-- In this example, we create an array combining the SSN and the project name.
-- =======================================================================================

SELECT 
  SSN,
  project_name,
  array(SSN, project_name) AS project_info_array
FROM ${user_schema}.employee_project_flat;


-- COMMAND ----------

-- =======================================================================================
-- struct(): Combine multiple columns into a nested row object
-- =======================================================================================
-- This function creates a single struct (object-like) column from multiple fields.
--
-- Use case: Useful when you want to return nested structures — for example,
-- building a column that acts like a mini-record inside a row.
-- This is very handy when preparing for JSON export or working with nested schemas.
--
-- Here, we combine project name and SSN into a struct called project_info.
-- =======================================================================================

SELECT 
  SSN,
  project_name,
  struct(project_name, SSN) AS project_info
FROM ${user_schema}.employee_project_flat;


-- COMMAND ----------

-- =======================================================================================
-- map(): Create a key-value pair structure from columns
-- =======================================================================================
-- This function creates a map (dictionary-style structure) where one column is the key
-- and another column is the value — all within a single column.
--
-- Use case: Ideal for exporting configurations, tags, metadata or labeled values.
-- It’s also useful when you want to send dynamic attributes in structured formats 
-- like JSON or process flexible schemas downstream.
--
-- In this example, we build a map where the project name is the key, and SSN is the value.
-- =======================================================================================

SELECT 
  SSN,
  project_name,
  map(project_name, SSN) AS project_map
FROM ${user_schema}.employee_project_flat;


-- COMMAND ----------

-- MAGIC %python
-- MAGIC # ---------------------------------------------------------------------------------------
-- MAGIC # Example: Running a collect_list() SQL query in Python and processing the result
-- MAGIC # ---------------------------------------------------------------------------------------
-- MAGIC
-- MAGIC # Execute SQL query
-- MAGIC query = f"""
-- MAGIC     SELECT 
-- MAGIC       SSN,
-- MAGIC       collect_list(project_name) AS projects
-- MAGIC     FROM {dbutils.widgets.get("user_schema")}.employee_project_flat
-- MAGIC     GROUP BY SSN
-- MAGIC """
-- MAGIC df_result = spark.sql(query)
-- MAGIC
-- MAGIC # Convert to a list of Python dictionaries (one per row)
-- MAGIC result = df_result.collect()
-- MAGIC
-- MAGIC # Example: Accessing one row programmatically
-- MAGIC for row in result:
-- MAGIC     print(f"Employee {row['SSN']} has projects: {row['projects']}")
-- MAGIC

-- COMMAND ----------

-- REVIEW QUESTIONS --
-- The following questions are recommended to reinforce the concepts from this notebook.

-- 1. What is the purpose of the collect_list() function in Spark SQL?
-- 2. How does collect_set() differ from collect_list()?
-- 3. What is the benefit of converting multiple rows into a single array?
-- 4. How can you group records by a column and aggregate their values as arrays?
-- 5. What use cases require transforming a table into an array-like structure?
-- 6. How can you build a key-value map using SQL in Spark?
-- 7. What is the difference between array_agg() and collect_list()?
-- 8. Why might nested structures be useful in reporting or APIs?
-- 9. Can arrays be created directly from a SELECT without GROUP BY?
-- 10. What are some risks when aggregating to arrays (e.g., duplicates, ordering)?

-- -----------------------------------------------------------------------

-- ANSWERS --

-- 1. To aggregate values from multiple rows into a single array grouped by some key.
-- 2. collect_list() includes duplicates; collect_set() removes duplicates.
-- 3. To simplify downstream processing, serialize grouped data, or prepare JSON-like structures.
-- 4. Using GROUP BY with collect_list() or collect_set() in the SELECT clause.
-- 5. Use cases include building documents, exporting data for APIs, or structuring inputs for machine learning.
-- 6. Using map_from_entries() or a combination of arrays of structs with key/value pairs.
-- 7. array_agg() is ANSI SQL standard and can maintain order in some implementations; collect_list() is native to Spark.
-- 8. They help represent one-to-many relationships compactly and are easy to use in external systems.
-- 9. Yes, but the array would contain all values in the dataset rather than grouped by any column.
-- 10. You might lose important context, introduce duplicates, or encounter unpredictable element order.
