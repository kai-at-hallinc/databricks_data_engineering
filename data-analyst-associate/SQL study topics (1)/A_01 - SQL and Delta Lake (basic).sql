-- Databricks notebook source
/*
====================================================================
SQL & Delta Lake Workshop - Chapter 1: Introduction to Delta Tables
====================================================================

This notebook is part of a hands-on workshop designed to teach the essentials of working with Delta Lake on Databricks. 
It introduces key concepts through practical examples, including creating schemas, working with Delta tables, exploring time travel, 
and understanding how transaction logs and storage optimizations work.

--------------------------------------------------------------------
SECTION 0 - ENVIRONMENT SETUP (Executed in the first cell)
--------------------------------------------------------------------
The first cell in this notebook is critical. It configures the user-specific environment 
so that each participant can work independently without conflicts.

It performs the following actions:

1. Extracts the current user's email address and normalizes it into a safe identifier (e.g., peter.spider@company.com -> peter_spider).
2. Constructs a unique schema name (e.g., `sql_review_peter_spider`) and a personal S3 storage path for the user 
   (e.g., `s3://databricks-hatchworks-main-bucket/external_data/peter_spider/sql_review`).
3. Ensures the schema and folder exist (creating them if needed).
4. Sets two widgets: `user_schema` and `user_path`, which are used throughout the notebook.
5. This cell must be executed before anything else in the notebook to ensure correct operation.

--------------------------------------------------------------------
SECTION 1 - CREATING TEMPORARY VIEWS AND DELTA TABLES
--------------------------------------------------------------------
- Demonstrates how to create a temporary view using a `SELECT ... UNION ALL` construct.
- Creates a Delta table named `employee_delta` based on the temporary view.
- Shows how to query both the temporary view and the Delta table.

--------------------------------------------------------------------
SECTION 2 - MODIFYING THE TABLE AND GENERATING HISTORY
--------------------------------------------------------------------
- Runs multiple `UPDATE` and `MERGE` operations on the Delta table to simulate real-world changes.
- Demonstrates Delta Lake's time travel capability via:
  - `DESCRIBE HISTORY`
  - `VERSION AS OF` queries
  - CTE-based version queries

--------------------------------------------------------------------
SECTION 3 - OPTIMIZING AND CLEANING DELTA STORAGE
--------------------------------------------------------------------
- Explains and runs `OPTIMIZE` to consolidate small files for better performance.
- Shows how to safely use `VACUUM` with 0-hour retention by:
  - Disabling the Delta safety check.
  - Setting table properties for log and deleted file retention.
  - Running `VACUUM RETAIN 0 HOURS` to aggressively clean up unused files.

--------------------------------------------------------------------
SECTION 4 - VALIDATION AND CLEANUP
--------------------------------------------------------------------
- Revisits `DESCRIBE HISTORY` to confirm operations.
- Notes that history metadata still remains even after `VACUUM`.
- Optional: Dropping the table at the end if needed.

--------------------------------------------------------------------
NOTES
--------------------------------------------------------------------
- This notebook is designed to be user-isolated. Every user works within their own schema and S3 folder.
- It is safe to rerun the entire notebook multiple times without breaking the setup.
- All operations are fully auditable and reversible using Delta Lake features.
- The notebook is modular and can be adapted for other datasets or extended with advanced Delta features.

====================================================================
End of Introduction
====================================================================
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

SELECT 
  '${user_schema}' AS user_schema,
  '${user_path}' AS user_path;


-- COMMAND ----------


USE ${user_schema}; -- We need to use the previously created scheme

/*
Temporary views in Databricks are session-scoped and will be dropped at the end of the session. 
They are useful for intermediate transformations and do not persist data to storage.
*/
CREATE OR REPLACE TEMP VIEW tmp_view_employee AS
SELECT 
  CAST ('AAAAA' AS varchar(5)) AS ssn, 
  'Amy' AS first_name, 
  'Reina' AS last_name, 
  5000 AS salary, 
  CAST ('2025-01-01' AS DATE) AS entry_date UNION ALL
SELECT 'BBBBB', 'Bruce','Trillo', 6000, CAST ('2025-02-01' AS DATE)  UNION ALL
SELECT 'CCCCC', 'Carla', 'Rivera', 7000, CAST ('2025-03-01' AS DATE) ;

-- COMMAND ----------

/*
We can query the temporary view
*/
SELECT * FROM tmp_view_employee ORDER BY ssn;



-- COMMAND ----------

/*
Delta table creation
*/
CREATE OR REPLACE TABLE employee_delta AS
SELECT * FROM tmp_view_employee;

-- COMMAND ----------

-- This command lists all tables/views in the specified schema.
SHOW TABLES IN ${user_schema};


-- COMMAND ----------

-- DESCRIBE DETAIL
-- ----------------
-- This command returns detailed metadata about a Delta table, including:
--   - Table format (should be "delta")
--   - Physical storage location (S3 path)
--   - Creation time and size
--   - Number of files and rows (if statistics are available)
--
-- It's useful for auditing and understanding where the table lives and how it's structured internally.

DESCRIBE DETAIL employee_delta;


-- COMMAND ----------


USE ${user_schema}; -- We need to use the previously created scheme

/*
Querying the Delta table
*/

select * from employee_delta ORDER BY ssn;

-- COMMAND ----------


select * from ${user_schema}.employee_delta ORDER BY ssn; -- same query, but using the schema created in the previous cell

-- COMMAND ----------


USE ${user_schema}; -- We need to use the previously created scheme

MERGE INTO employee_delta AS tget
USING (
    SELECT ssn, salary * 1.10 AS new_salary 
    FROM employee_delta 
    WHERE ssn = 'CCCCC'
) AS src
ON tget.ssn = src.ssn
WHEN MATCHED THEN 
UPDATE SET tget.salary = src.new_salary;

-- COMMAND ----------


USE ${user_schema}; -- We need to use the previously created scheme

SELECT * FROM employee_delta;

-- COMMAND ----------


USE ${user_schema}; -- We need to use the previously created scheme

DESCRIBE HISTORY employee_delta; -- show history of the table

-- COMMAND ----------


USE ${user_schema}; -- We need to use the previously created scheme

SELECT * FROM (DESCRIBE HISTORY employee_delta)
WHERE version BETWEEN 2 and 4 ORDER BY version DESC; -- show history of the table, only version 2 and 4

-- COMMAND ----------


USE ${user_schema}; -- We need to use the previously created scheme

;WITH q AS ( 
  DESCRIBE HISTORY employee_delta )
SELECT * 
FROM q 
WHERE version = 1 

-- using a CTE, querying the table, only version 2
-- See CTE usage in later notebooks

-- COMMAND ----------


USE ${user_schema}; -- We need to use the previously created scheme

SELECT * FROM employee_delta VERSION AS OF 1; -- Querying the table, using version 1

-- COMMAND ----------


USE ${user_schema}; -- We need to use the previously created scheme

SELECT '1' as vrsn, * FROM employee_delta VERSION AS OF 1
UNION ALL
SELECT '0' as vrsn, * FROM employee_delta VERSION AS OF 0 -- Querying the table, using versions and unions
ORDER BY ssn, vrsn;

-- COMMAND ----------


USE ${user_schema}; -- We need to use the previously created scheme

SELECT * FROM employee_delta VERSION AS OF 10; -- be careful, we only have 0 and 1 versions

-- COMMAND ----------


USE ${user_schema}; -- We need to use the previously created scheme

SELECT * FROM employee_delta VERSION AS OF 1;

-- COMMAND ----------


USE ${user_schema}; -- We need to use the previously created scheme

-- New update (we need more history)
UPDATE employee_delta SET salary = salary * 1.10 WHERE ssn <> 'CCCCC'

-- COMMAND ----------


USE ${user_schema}; -- We need to use the previously created scheme

DESCRIBE HISTORY employee_delta

-- COMMAND ----------


USE ${user_schema}; -- We need to use the previously created scheme

-- New update (we need more history)
UPDATE employee_delta SET salary = salary * 1.11 WHERE ssn = 'AAAAA';
UPDATE employee_delta SET salary = salary * 1.13 WHERE ssn = 'BBBBB';
UPDATE employee_delta SET salary = salary * 1.17 WHERE ssn = 'CCCCC';

-- COMMAND ----------


USE ${user_schema}; -- We need to use the previously created scheme

DESCRIBE HISTORY employee_delta

-- COMMAND ----------


USE ${user_schema}; -- We need to use the previously created scheme

-- The OPTIMIZE command is used to compact small files within a Delta table into larger files.
-- This can improve query performance by reducing the number of files read during a query.
-- However, it can be resource-intensive and should be used during off-peak hours.
-- It is also recommended to ensure there is enough disk space available before running this command.
-- Check "metrics" column.

OPTIMIZE employee_delta;

-- COMMAND ----------


USE ${user_schema}; -- We need to use the previously created scheme

-- Disable safety check that enforces a 7-day minimum retention period for VACUUM.
-- This allows us to run VACUUM ... RETAIN 0 HOURS to immediately delete old files.
-- Use with caution: this may permanently remove the ability to time travel.
SET spark.databricks.delta.retentionDurationCheck.enabled = false;


-- The ALTER TABLE command is used to change the properties of an existing Delta table.
-- The 'delta.logRetentionDuration' property specifies how long the transaction log history is kept.
-- Setting it to 'interval 0 hours' means the transaction log history will be retained for 0 hours.
-- The 'delta.deletedFileRetentionDuration' property specifies how long the deleted files are retained.
-- Setting it to 'interval 0 hours' means the deleted files will be retained for 0 hours.

ALTER TABLE employee_delta SET TBLPROPERTIES (
  'delta.logRetentionDuration' = 'interval 0 hours',
  'delta.deletedFileRetentionDuration' = 'interval 0 hours'
);

-- The VACUUM command is used to clean up old files from a Delta table.
-- The 'RETAIN 0 HOURS' option specifies that files older than 0 hours should be removed.
VACUUM employee_delta RETAIN 0 HOURS;

-- COMMAND ----------


USE ${user_schema}; -- We need to use the previously created scheme

DESCRIBE HISTORY employee_delta; -- show history of the table

-- COMMAND ----------

-- REVIEW QUESTIONS -- 
-- The following questions are recommended to reinforce the concepts from this notebook.

-- 1. What command is used to create a table in Delta Lake using SQL?
-- 2. What is the difference between a managed and an external Delta table?
-- 3. What is the default file format when using Delta Lake?
-- 4. How does Delta Lake handle schema enforcement?
-- 5. How do you insert multiple rows into a Delta table using SQL?
-- 6. What are the benefits of using Delta Lake over traditional data lake formats like Parquet or CSV?
-- 7. Can you query a Delta table using standard SQL SELECT statements?
-- 8. What command is used to inspect the structure of a table in SQL?

-- -----------------------------------------------------------------------

-- ANSWERS --

-- 1. CREATE TABLE <table_name> USING DELTA AS SELECT ... or CREATE TABLE ... USING DELTA.
-- 2. Managed tables store data in the default location controlled by Databricks; external tables use a user-defined path.
-- 3. Delta (a transactional format built on top of Parquet) is the default format.
-- 4. Delta Lake enforces schema by default and throws errors if data does not match the expected schema.
-- 5. Use INSERT INTO <table_name> VALUES (...), (...), (...).
-- 6. Delta Lake provides ACID transactions, schema enforcement, time travel, and better performance through optimizations.
-- 7. Yes, Delta tables support standard SQL queries like SELECT, WHERE, etc.
-- 8. Use DESCRIBE TABLE <table_name> or DESCRIBE EXTENDED <table_name>.
