-- Databricks notebook source
/*  
==============================================================================
E_02 - REAL-TIME ERROR HANDLING IN DELTA LAKE
==============================================================================

OBJECTIVE
------------------------------------------------------------------------------
This notebook demonstrates how to handle data insertion errors in real time 
using Delta Lake in Databricks. It shows how to simulate a real-time data 
stream, validate incoming records, capture insertion errors, and log them 
for further analysis and recovery.

SCENARIO
------------------------------------------------------------------------------
In a production environment, data often arrives in unpredictable formats.
Some records may contain invalid types, corrupted fields, or business rule 
violations (e.g., negative salary, null department, malformed SSNs).

Instead of failing the entire ingestion pipeline, the system should:
- Insert only valid records into the main Delta table
- Capture invalid records in an error log table
- Enable recovery of corrected records at a later stage

WORKFLOW OVERVIEW
------------------------------------------------------------------------------
1. Simulate a real-time data batch using `Row()` with valid and invalid records
2. Loop through each record, apply validations:
   - SSN must be numeric
   - Salary must be a positive number
   - Department must not be null
3. If the record passes, insert it into the main Delta table:
   - If it already exists, perform a `MERGE` (upsert)
   - Otherwise, insert as new
4. If the record fails, capture the error message and store it in the `error_log` table
5. Review the contents of both tables to confirm
6. Optional: Recover and reprocess valid records from the error log table

DELTA TABLES INVOLVED
------------------------------------------------------------------------------
- `${user_schema}.processed_data`: stores successfully validated records
- `${user_schema}.error_log`: stores rejected records along with error descriptions

NOTES
------------------------------------------------------------------------------
- This notebook avoids crashing the pipeline due to bad data
- Business rules are enforced at runtime using Python validations
- Error messages are stored for observability and troubleshooting
- Schema auto-merge is disabled by default in Unity Catalog with ACLs, so table structure 
  must be consistent across operations

CONCLUSION
------------------------------------------------------------------------------
This approach to error handling improves the robustness of real-time ingestion 
pipelines by capturing and isolating bad records for later review, 
instead of losing data or interrupting processing.

==============================================================================
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
-- TABLE CREATION FOR ERROR HANDLING STRATEGY
-- ==============================================================================

-- Table for valid, successfully processed records
CREATE OR REPLACE TABLE ${user_schema}.processed_data (
    ssn INT,
    first_name STRING,
    salary INT,
    department STRING
) USING DELTA;

-- Table for records that failed validation or casting, along with error notes
CREATE OR REPLACE TABLE ${user_schema}.error_log (
    ssn STRING,
    first_name STRING,
    salary STRING,
    department STRING,
    error_msg STRING
) USING DELTA;

-- ==============================================================================
-- TEMPORARY STAGING VIEW FOR DATA INGESTION
-- ==============================================================================
-- All columns are treated as STRING initially to prevent runtime casting failures.
-- Error messages are simulated for rows with known data issues.
-- ==============================================================================

CREATE OR REPLACE TEMP VIEW view_staging_employees AS 
SELECT * FROM VALUES
    ('11111', 'Amy', '5000', 'Sales', NULL),
    ('22222', 'Bruce', '6000', 'IT', NULL),
    ('33333', 'Carla', '7000', 'HR', NULL),
    ('44444', 'Pedro', '-1000', 'Finance', 'Negative salary'),
    ('error', 'Maria', '8000', 'IT', 'SSN is not numeric'),
    ('66666', 'Elena', '9000', NULL, 'Missing department')
AS staging(ssn, first_name, salary, department, error_msg);

-- ==============================================================================
-- LOAD VALID DATA INTO `processed_data`
-- ==============================================================================
-- Records must pass regex validation and have no simulated error messages.
-- ==============================================================================

INSERT INTO ${user_schema}.processed_data 
SELECT 
    CAST(ssn AS INT), 
    first_name, 
    CAST(salary AS INT), 
    department
FROM view_staging_employees
WHERE error_msg IS NULL 
  AND ssn RLIKE '^[0-9]+$' 
  AND salary RLIKE '^-?[0-9]+$';

-- ==============================================================================
-- LOAD INVALID DATA INTO `error_log`
-- ==============================================================================
-- This includes rows with error annotations or failing basic validations.
-- ==============================================================================

INSERT INTO ${user_schema}.error_log
SELECT ssn, first_name, salary, department, error_msg
FROM view_staging_employees
WHERE error_msg IS NOT NULL 
   OR NOT (ssn RLIKE '^[0-9]+$' AND salary RLIKE '^-?[0-9]+$');




-- COMMAND ----------

-- ==============================================================================
-- PREVIEW DATA FROM BOTH TABLES
-- ==============================================================================
SELECT * FROM ${user_schema}.processed_data;


-- COMMAND ----------

SELECT * FROM ${user_schema}.error_log;

-- COMMAND ----------

-- ==============================================================================
-- RECOVERY AND REINSERTION OF CLEANED DATA
-- ==============================================================================
-- Attempt to reinsert fixed records from the error log into the main Delta table.
-- ==============================================================================

INSERT INTO ${user_schema}.processed_data
SELECT 
    CAST(ssn AS INT), 
    first_name, 
    ABS ( CAST(salary AS INT) ), -- Business Rules: abs(salary) 
    department
FROM ${user_schema}.error_log
WHERE ssn RLIKE '^[0-9]+$' 
  AND salary RLIKE '^-?[0-9]+$'
  AND department IS NOT NULL;

-- ==============================================================================
-- CLEANUP OF RECOVERED RECORDS FROM THE ERROR LOG
-- ==============================================================================
-- Remove successfully reprocessed records from the error table.
-- ==============================================================================

DELETE FROM ${user_schema}.error_log
WHERE ssn RLIKE '^[0-9]+$' 
  AND salary RLIKE '^-?[0-9]+$'
  AND department IS NOT NULL;



-- COMMAND ----------

-- ==============================================================================
-- VERIFY THAT THE RECORDS WERE RECOVERED CORRECTLY
-- ==============================================================================
SELECT * FROM ${user_schema}.processed_data;



-- COMMAND ----------

SELECT * FROM ${user_schema}.error_log;

-- We can proceed as in the previous notebook.
-- Update the log table and merge it with the main table.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql import Row
-- MAGIC from pyspark.sql.utils import AnalysisException
-- MAGIC
-- MAGIC # ------------------------------------------------------------------
-- MAGIC # Simulated real-time data (some valid, some with errors)
-- MAGIC # ------------------------------------------------------------------
-- MAGIC data = [
-- MAGIC     Row(ssn="11111", first_name="Amy", salary=5000, department="Sales"),
-- MAGIC     Row(ssn="22222", first_name="Bruce", salary=6000, department="IT"),
-- MAGIC     Row(ssn="33333", first_name="Carla", salary=7000, department="HR"),
-- MAGIC     Row(ssn="44444", first_name="Pedro", salary=-1000, department="Finance"),  # Invalid: salary
-- MAGIC     Row(ssn="error", first_name="Maria", salary=8000, department="IT"),        # Invalid: SSN
-- MAGIC     Row(ssn="55555", first_name="Elena", salary=9000, department=None)         # Invalid: null dept
-- MAGIC ]
-- MAGIC
-- MAGIC # ------------------------------------------------------------------
-- MAGIC # Capture errors
-- MAGIC # ------------------------------------------------------------------
-- MAGIC errors = []
-- MAGIC
-- MAGIC # ------------------------------------------------------------------
-- MAGIC # Process each record
-- MAGIC # ------------------------------------------------------------------
-- MAGIC for row in data:
-- MAGIC     try:
-- MAGIC         # Extract and validate fields
-- MAGIC         ssn = str(row.ssn)
-- MAGIC         fname = row.first_name.replace("'", "")
-- MAGIC         dept = f"'{row.department}'" if row.department else "NULL"
-- MAGIC         salary = int(row.salary)
-- MAGIC
-- MAGIC         if not ssn.isdigit():
-- MAGIC             raise ValueError("SSN must be numeric.")
-- MAGIC         if salary < 0:
-- MAGIC             raise ValueError("Salary cannot be negative.")
-- MAGIC         if row.department is None:
-- MAGIC             raise ValueError("Department cannot be null.")
-- MAGIC
-- MAGIC         # ------------------------------------------------------------------
-- MAGIC         # Check if SSN exists
-- MAGIC         # ------------------------------------------------------------------
-- MAGIC         ssn_exists = spark.sql(f"""
-- MAGIC             SELECT COUNT(*) FROM {dbutils.widgets.get("user_schema")}.processed_data
-- MAGIC             WHERE ssn = '{ssn}'
-- MAGIC         """).collect()[0][0] > 0
-- MAGIC
-- MAGIC         if ssn_exists:
-- MAGIC             # Update existing record
-- MAGIC             spark.sql(f"""
-- MAGIC                 MERGE INTO {dbutils.widgets.get("user_schema")}.processed_data AS target
-- MAGIC                 USING (SELECT '{ssn}' AS ssn, '{fname}' AS first_name, {salary} AS salary, {dept} AS department) AS source
-- MAGIC                 ON target.ssn = source.ssn
-- MAGIC                 WHEN MATCHED THEN UPDATE SET
-- MAGIC                     first_name = source.first_name,
-- MAGIC                     salary = source.salary,
-- MAGIC                     department = source.department
-- MAGIC             """)
-- MAGIC         else:
-- MAGIC             # Insert new record
-- MAGIC             spark.sql(f"""
-- MAGIC                 INSERT INTO {dbutils.widgets.get("user_schema")}.processed_data
-- MAGIC                 VALUES ('{ssn}', '{fname}', {salary}, {dept})
-- MAGIC             """)
-- MAGIC
-- MAGIC     except (AnalysisException, ValueError, Exception) as e:
-- MAGIC         errors.append((
-- MAGIC             str(row.ssn),
-- MAGIC             row.first_name,
-- MAGIC             str(row.salary),
-- MAGIC             row.department,
-- MAGIC             str(e)
-- MAGIC         ))
-- MAGIC
-- MAGIC # ------------------------------------------------------------------
-- MAGIC # Save any failed records to the error_log table
-- MAGIC # ------------------------------------------------------------------
-- MAGIC if errors:
-- MAGIC     df_errors = spark.createDataFrame(
-- MAGIC         errors, 
-- MAGIC         ["ssn", "first_name", "salary", "department", "error_msg"]
-- MAGIC     )
-- MAGIC     df_errors.write.format("delta") \
-- MAGIC         .mode("append").option("mergeSchema", "true") \
-- MAGIC         .saveAsTable(f"{dbutils.widgets.get('user_schema')}.error_log")
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # ------------------------------------------------------------------
-- MAGIC # Verification of results
-- MAGIC # ------------------------------------------------------------------
-- MAGIC print("Valid records inserted:")
-- MAGIC spark.sql(f"SELECT * FROM {dbutils.widgets.get('user_schema')}.processed_data").show()
-- MAGIC
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC print("Records with errors:")
-- MAGIC spark.sql(f"SELECT * FROM {dbutils.widgets.get('user_schema')}.error_log").show()
-- MAGIC from pyspark.sql.utils import AnalysisException
-- MAGIC
-- MAGIC # We can proceed as in the previous notebook.
-- MAGIC # Update the log table and merge it with the main table.

-- COMMAND ----------

-- REVIEW QUESTIONS --
-- The following questions are recommended to reinforce the concepts from this notebook.

-- 1. What strategies can be used to detect invalid data during ingestion or processing?
-- 2. How can you filter out invalid records in a DataFrame using PySpark?
-- 3. Why is it important to separate valid and invalid records during processing?
-- 4. What is the role of the CASE WHEN clause in data validation?
-- 5. How does `withColumn` help in real-time error tagging?
-- 6. What function is used to combine DataFrames with the same schema?
-- 7. What happens if invalid data is written to a Delta table with constraints?
-- 8. How can you track how many invalid records were received in a batch?
-- 9. How does this notebook simulate real-time processing in a batch context?
-- 10. Why is it a good practice to maintain a separate table for rejected records?

-- -----------------------------------------------------------------------

-- ANSWERS --

-- 1. Using filters, conditional logic (`CASE`), schema enforcement, and validation rules.
-- 2. Using `filter()` with boolean expressions like `salary > 0`, or by tagging and filtering.
-- 3. To avoid polluting clean datasets, simplify debugging, and allow separate correction workflows.
-- 4. It enables conditional tagging or logic depending on whether a row meets expected criteria.
-- 5. It allows adding new columns that mark whether data is valid or invalid in real time.
-- 6. `unionByName()` – combines DataFrames with matching column names.
-- 7. The write will fail with a constraint violation unless handled upstream.
-- 8. By counting the rows in the invalid DataFrame or saving to an audit table.
-- 9. By processing a batch file as if it were a stream, applying validation logic before writing.
-- 10. To allow auditing, reprocessing, and analytics on failed rows without affecting clean data.
