-- Databricks notebook source
/*  
=======================================================================================
CHAPTER E_01 - Monitoring, Validation, and Data Correction in Delta Lake
=======================================================================================

OBJECTIVE:
---------------------------------------------------------------------------------------
This notebook demonstrates how to implement advanced monitoring and validation 
strategies on Delta Lake tables. Unlike CHECK CONSTRAINTS that enforce strict rules, 
these techniques allow for flexible and real-time detection of anomalies without 
interrupting data ingestion or operational pipelines.

WORKFLOW OVERVIEW:
---------------------------------------------------------------------------------------
1. Create a Delta table with sample employee data (including invalid entries)
2. Dynamically monitor salary ranges and detect anomalies (e.g., out-of-range values)
3. Use CASE statements and custom queries for real-time data classification
4. Store suspicious records in an audit table (log_anomalies) for further review
5. Automatically correct invalid records using business rules via `UPDATE` or `MERGE`
6. Clean up the audit log after successful validation

HIGHLIGHTS:
---------------------------------------------------------------------------------------
- Demonstrates validation logic without using rigid constraints
- Focuses on observability and data quality in production scenarios
- Allows organizations to respond to anomalies instead of blocking ingestion
- Supports automation and audit logging for traceability

USE CASES:
---------------------------------------------------------------------------------------
- Financial pipelines needing salary range validation
- ETL processes where soft rules are required instead of hard enforcement
- Operational monitoring for regulatory compliance

RECOMMENDATION:
---------------------------------------------------------------------------------------
Use this approach when business logic requires visibility over bad data
without rejecting or interrupting ingestion. Combine with constraints for
hybrid enforcement strategies.

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

-- ==============================================================================
-- CREATE DELTA TABLE FOR ADVANCED MONITORING
-- ==============================================================================
-- This table contains employee records where we'll apply validations and 
-- real-time monitoring without affecting data ingestion.
-- ==============================================================================

CREATE OR REPLACE TABLE ${user_schema}.monitoring_validation (
    ssn STRING,
    first_name STRING,
    salary INT,
    department STRING
) USING DELTA;

-- ==============================================================================
-- INSERT SAMPLE RECORDS
-- ==============================================================================
INSERT INTO ${user_schema}.monitoring_validation VALUES
('AAAAA', 'Amy',     2500, 'Sales'),
('BBBBB', 'Bruce',   5000, 'IT'),
('CCCCC', 'Carla',  10000, 'HR'),
('DDDDD', 'Pedro',  -2000, 'Finance'), 
('EEEEE', 'Danielle',   8000, 'Marketing');

-- ==============================================================================
-- VERIFY INSERTED RECORDS
-- ==============================================================================
SELECT * FROM ${user_schema}.monitoring_validation;

-- COMMAND ----------

-- ==============================================================================
-- MONITORING DATA WITH DYNAMIC VALIDATIONS
-- ==============================================================================
-- Records are categorized based on their salary range without enforcing constraints.
-- This helps identify anomalies without blocking the ingestion process.
-- ==============================================================================

SELECT ssn, first_name, salary, department,
       CASE 
           WHEN salary < 3000 THEN 'LOW'
           WHEN salary BETWEEN 3000 AND 7000 THEN 'NORMAL'
           ELSE 'HIGH' 
       END AS salary_category
FROM ${user_schema}.monitoring_validation;

-- COMMAND ----------

-- ==============================================================================
-- DETECTING INCONSISTENT DATA
-- ==============================================================================
-- Identifies records with values outside expected salary ranges.
-- ==============================================================================

SELECT * 
FROM ${user_schema}.monitoring_validation
WHERE salary < 0 OR salary > 10000;


-- COMMAND ----------

-- ==============================================================================
-- CREATE AUDIT TABLE FOR INCONSISTENT DATA
-- ==============================================================================
-- Instead of blocking incorrect data, store it in an audit table for review.
-- ==============================================================================

CREATE TABLE IF NOT EXISTS ${user_schema}.log_anomalies (
    ssn STRING, 
    first_name STRING, 
    salary INT, 
    department STRING,
    description STRING
) USING DELTA;

-- ==============================================================================
-- INSERT INCONSISTENT RECORDS INTO THE AUDIT TABLE
-- ==============================================================================
-- Detected anomalies are logged without interrupting main data flow.
-- ==============================================================================

INSERT INTO ${user_schema}.log_anomalies
SELECT ssn, first_name, salary, department, 'Salary out of expected range'
FROM ${user_schema}.monitoring_validation
WHERE salary < 0 OR salary > 10000;

-- ==============================================================================
-- REVIEW ANOMALIES LOGGED FOR AUDIT
-- ==============================================================================
SELECT * FROM ${user_schema}.log_anomalies;


-- COMMAND ----------

-- ==============================================================================
-- CORRECTING INVALID DATA IN THE MAIN TABLE
-- ==============================================================================
-- Fixes values flagged by the audit logic based on business rules.
-- ==============================================================================

UPDATE ${user_schema}.monitoring_validation 
SET salary = 3000  -- Adjusted according to business rule for minimum salary
WHERE salary < 0;

UPDATE ${user_schema}.monitoring_validation 
SET salary = 10000  -- Adjusted according to business rule for maximum salary
WHERE salary > 10000;

-- ==============================================================================
-- VERIFY THAT THE VALUES HAVE BEEN FIXED
-- ==============================================================================
SELECT * FROM ${user_schema}.monitoring_validation;


-- COMMAND ----------

-- ==============================================================================
-- AUTOMATED AUDIT AND CORRECTION USING MERGE
-- ==============================================================================
-- Merges audit data with the main table.
-- If a record has invalid values, it is automatically corrected.
-- ==============================================================================

MERGE INTO ${user_schema}.monitoring_validation AS target
USING ${user_schema}.log_anomalies AS source
ON target.ssn = source.ssn
WHEN MATCHED AND target.salary < 0 THEN 
    UPDATE SET target.salary = 3000;  -- Corrected based on business rules (minimum salary)

MERGE INTO ${user_schema}.monitoring_validation AS target
USING ${user_schema}.log_anomalies AS source
ON target.ssn = source.ssn
WHEN MATCHED AND target.salary > 10000 THEN 
    UPDATE SET target.salary = 10000;  -- Corrected based on business rules (maximum salary)

-- ==============================================================================
-- VERIFY THAT THE AUTOMATED CORRECTIONS WERE APPLIED
-- ==============================================================================
SELECT * FROM ${user_schema}.monitoring_validation;


-- COMMAND ----------

-- ==============================================================================
-- CLEANUP OF THE AUDIT TABLE AFTER DATA CORRECTION
-- ==============================================================================
-- All records are removed from the audit table after they have been
-- successfully corrected in the main table.
-- ==============================================================================

TRUNCATE TABLE ${user_schema}.log_anomalies;

-- ==============================================================================
-- VERIFY THAT THE AUDIT TABLE IS NOW EMPTY
-- ==============================================================================
SELECT * FROM ${user_schema}.log_anomalies;


-- COMMAND ----------

-- REVIEW QUESTIONS --
-- The following questions are recommended to reinforce the concepts from this notebook.

-- 1. Why is it important to validate data after ingestion?
-- 2. What types of data quality issues are commonly checked in a Delta Lake workflow?
-- 3. How can NULL values be detected and filtered in SQL?
-- 4. How can you detect duplicate entries in a Delta table?
-- 5. What is the purpose of separating valid and invalid rows into temporary views?
-- 6. How can you correct invalid records after ingestion?
-- 7. What operations are typically used to fix invalid or corrupted data?
-- 8. How do you ensure that corrections do not introduce new inconsistencies?
-- 9. What are the benefits of using Delta Lake for monitoring and correction workflows?
-- 10. How can you audit which rows were corrected or removed during validation?

-- -----------------------------------------------------------------------

-- ANSWERS --

-- 1. To ensure data quality, prevent downstream errors, and maintain reliability in analytics and decision-making.
-- 2. NULL values, duplicates, schema mismatches, incorrect formats, or business rule violations.
-- 3. Using `WHERE column IS NULL` or `WHERE column IS NOT NULL`.
-- 4. Using `GROUP BY` and `HAVING COUNT(*) > 1`, or window functions like `row_number() OVER (...)`.
-- 5. To isolate records for inspection and separate correction logic from production data.
-- 6. Using `UPDATE` to fix values, `DELETE` to remove bad rows, or reinserting clean data.
-- 7. UPDATE, DELETE, and INSERT statements.
-- 8. By validating the results with additional SELECT filters or integrity checks after the fix.
-- 9. Delta Lake provides full version history, ACID compliance, and allows easy rollback or recovery.
-- 10. By using `DESCRIBE HISTORY` and tracking before/after values manually or with audit tables.
