# Databricks notebook source
# MAGIC %sql
# MAGIC /*  
# MAGIC ==============================================================================
# MAGIC DATA AUDIT AND QUALITY IN DELTA LAKE
# MAGIC ==============================================================================
# MAGIC
# MAGIC -- IMPORTANCE OF DATA AUDIT AND QUALITY
# MAGIC -- In modern data environments, it is essential to ensure the quality, integrity, and reliability of the stored information.
# MAGIC -- Delta Lake offers robust mechanisms for data auditing and validation, ensuring consistency and detecting anomalies.
# MAGIC
# MAGIC ==============================================================================
# MAGIC -- AUDIT TOOLS IN DELTA LAKE
# MAGIC 1. DESCRIBE HISTORY: Allows tracking all modifications in a Delta table.
# MAGIC    Usage: 
# MAGIC    DESCRIBE HISTORY table_name;
# MAGIC
# MAGIC 2. DATA VERSIONING: Allows access to previous versions for auditing or recovery.
# MAGIC    Usage: 
# MAGIC    SELECT * FROM table_name VERSION AS OF 3;
# MAGIC    SELECT * FROM table_name TIMESTAMP AS OF '2025-01-01 12:00:00';
# MAGIC
# MAGIC 3. OPERATION LOGGING: Each executed operation is stored with user details and change metrics.
# MAGIC    Usage:
# MAGIC    SELECT * FROM system.information_schema.tables WHERE table_name = 'table_name';
# MAGIC
# MAGIC ==============================================================================
# MAGIC -- DATA QUALITY MECHANISMS IN DELTA LAKE
# MAGIC 1. CONSTRAINTS (Integrity constraints): Rules can be defined to validate data before inserting.
# MAGIC    Example:
# MAGIC    ALTER TABLE table_name ADD CONSTRAINT chk_salary CHECK (salary > 0);
# MAGIC
# MAGIC 2. STRICT SCHEMAS: Prevents unexpected changes in the table structure.
# MAGIC    Example:
# MAGIC    CREATE TABLE table_name (... columns ...) USING DELTA TBLPROPERTIES ('delta.columnMapping.mode' = 'name');
# MAGIC
# MAGIC 3. AUTOMATIC DATA VALIDATION: Validation rules can be defined with `CHECK`.
# MAGIC    Example:
# MAGIC    ALTER TABLE table_name ADD CONSTRAINT chk_department CHECK (department IN ('Sales', 'IT', 'HR'));
# MAGIC
# MAGIC ==============================================================================
# MAGIC -- PREVENTION OF CORRUPT DATA AND RECOVERY STRATEGIES
# MAGIC 1. USE OF TRANSACTIONS: Delta Lake handles ACID transactions, avoiding inconsistent states.
# MAGIC 2. ROLLBACK TO PREVIOUS VERSIONS: In case of corrupt data, a previous version can be restored.
# MAGIC    Usage:
# MAGIC    RESTORE TABLE table_name TO VERSION AS OF 5;
# MAGIC 3. ANOMALY DETECTION WITH AUTOMATIC VALIDATIONS:
# MAGIC    Usage:
# MAGIC    SELECT * FROM table_name WHERE salary < 0 OR department IS NULL;
# MAGIC
# MAGIC ==============================================================================
# MAGIC -- CONCLUSION
# MAGIC -- Delta Lake allows effective implementation of data auditing and quality.
# MAGIC -- The use of constraints, validations, and versioning ensures the integrity of the information.
# MAGIC -- It is recommended to use these tools in critical environments to avoid errors and data corruption.
# MAGIC ==============================================================================
# MAGIC */
# MAGIC
# MAGIC

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
# MAGIC -- CREATE DELTA TABLE FOR AUDIT AND DATA QUALITY VALIDATION
# MAGIC -- =======================================================================================
# MAGIC -- This table simulates an employee dataset and will be used to demonstrate:
# MAGIC -- - Constraints for data validation
# MAGIC -- - Audit tracking using Delta Lake features
# MAGIC -- =======================================================================================
# MAGIC
# MAGIC DROP TABLE IF EXISTS ${user_schema}.audit_data_quality;
# MAGIC
# MAGIC CREATE TABLE ${user_schema}.audit_data_quality (
# MAGIC     ssn STRING,
# MAGIC     first_name STRING,
# MAGIC     last_name STRING,
# MAGIC     salary INT,
# MAGIC     department STRING
# MAGIC ) USING DELTA;
# MAGIC
# MAGIC -- =======================================================================================
# MAGIC -- INSERT SAMPLE RECORDS FOR TESTING
# MAGIC -- =======================================================================================
# MAGIC -- These records are used to initialize the table and generate the first version.
# MAGIC -- =======================================================================================
# MAGIC
# MAGIC INSERT INTO ${user_schema}.audit_data_quality VALUES
# MAGIC ('AAAAA', 'Amy', 'Reina', 5000, 'Sales'),
# MAGIC ('BBBBB', 'Bruce', 'Trillo', 6000, 'IT'),
# MAGIC ('CCCCC', 'Carla', 'Rivera', 7000, 'HR');
# MAGIC
# MAGIC -- =======================================================================================
# MAGIC -- VERIFY INITIAL RECORDS
# MAGIC -- =======================================================================================
# MAGIC -- Display the current data in the Delta table for validation.
# MAGIC -- =======================================================================================
# MAGIC
# MAGIC SELECT * FROM ${user_schema}.audit_data_quality;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- =======================================================================================
# MAGIC -- ADD CONSTRAINTS TO ENFORCE DATA QUALITY
# MAGIC -- =======================================================================================
# MAGIC -- These constraints ensure that:
# MAGIC -- - Salary values must be greater than 0
# MAGIC -- - Only predefined department names are allowed
# MAGIC -- =======================================================================================
# MAGIC
# MAGIC ALTER TABLE ${user_schema}.audit_data_quality 
# MAGIC ADD CONSTRAINT chk_salary CHECK (salary > 0);
# MAGIC
# MAGIC ALTER TABLE ${user_schema}.audit_data_quality 
# MAGIC ADD CONSTRAINT chk_department CHECK (department IN ('Sales', 'IT', 'HR'));
# MAGIC
# MAGIC -- =======================================================================================
# MAGIC -- REVIEW TABLE PROPERTIES TO VERIFY CONSTRAINTS
# MAGIC -- =======================================================================================
# MAGIC -- This will show the applied constraints among other metadata.
# MAGIC -- =======================================================================================
# MAGIC
# MAGIC SHOW TBLPROPERTIES ${user_schema}.audit_data_quality;
# MAGIC
# MAGIC -- =======================================================================================
# MAGIC -- INTERPRETING CONSTRAINT METADATA IN TABLE PROPERTIES
# MAGIC -- =======================================================================================
# MAGIC -- The following properties confirm that the table now enforces two data quality rules:
# MAGIC -- 
# MAGIC -- delta.constraints.chk_salary:
# MAGIC --     Ensures that all salary values are greater than 0.
# MAGIC --
# MAGIC -- delta.constraints.chk_department:
# MAGIC --     Ensures that only valid departments ('Sales', 'IT', 'HR') are accepted.
# MAGIC --
# MAGIC -- These constraints are enforced at the Delta Lake level and any violation will trigger an error.
# MAGIC -- Additional metadata confirms that constraint validation is supported by the Delta engine.
# MAGIC -- =======================================================================================
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- =======================================================================================
# MAGIC -- ATTEMPT TO INSERT INVALID DATA TO TEST CONSTRAINT ENFORCEMENT
# MAGIC -- =======================================================================================
# MAGIC -- This insert should fail because the salary is negative (-3000),
# MAGIC -- violating the constraint: CHECK (salary > 0).
# MAGIC -- =======================================================================================
# MAGIC
# MAGIC INSERT INTO ${user_schema}.audit_data_quality 
# MAGIC VALUES ('PPPPP', 'Pedro', 'Gomez', -3000, 'Sales');
# MAGIC
# MAGIC -- =======================================================================================
# MAGIC -- CONSTRAINT VIOLATION: CHECK (salary > 0)
# MAGIC -- =======================================================================================
# MAGIC -- Delta Lake detected a data quality violation during the insert operation.
# MAGIC -- The error message clearly identifies the constraint that failed:
# MAGIC --     [DELTA_VIOLATE_CONSTRAINT_WITH_VALUES]
# MAGIC --     CHECK constraint `chk_salary (salary > 0)` violated by row with values:
# MAGIC --         - salary: -3000
# MAGIC --
# MAGIC -- This confirms that the `chk_salary` constraint is actively protecting the data,
# MAGIC -- preventing invalid values from being inserted into the table.
# MAGIC --
# MAGIC -- SQLSTATE: 23001 indicates an integrity constraint violation.
# MAGIC -- =======================================================================================
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- ==============================================================================
# MAGIC -- ATTEMPT TO INSERT INVALID DATA TO TEST CONSTRAINT ENFORCEMENT
# MAGIC -- ==============================================================================
# MAGIC -- This test tries to insert a record with a disallowed department.
# MAGIC -- It should fail due to the constraint: department IN ('Sales', 'IT', 'HR')
# MAGIC -- ==============================================================================
# MAGIC
# MAGIC INSERT INTO ${user_schema}.audit_data_quality VALUES 
# MAGIC ('MMMMM', 'Maria', 'Cruz', 6200, 'Marketing');
# MAGIC
# MAGIC
# MAGIC -- =======================================================================================
# MAGIC -- CONSTRAINT VIOLATION: CHECK (department IN ('Sales', 'IT', 'HR'))
# MAGIC -- =======================================================================================
# MAGIC -- Delta Lake rejected the row due to a violation of the `chk_department` constraint.
# MAGIC -- The attempted insert used a department value not allowed by the defined rule:
# MAGIC --     INSERTED department: 'Marketing'  → Not in the list ['Sales', 'IT', 'HR']
# MAGIC --
# MAGIC -- The error message would typically look like:
# MAGIC --     [DELTA_VIOLATE_CONSTRAINT_WITH_VALUES] 
# MAGIC --     CHECK constraint chk_department violated by row with values:
# MAGIC --         - department : 'Marketing'
# MAGIC --
# MAGIC -- This demonstrates that the `chk_department` constraint is actively validating 
# MAGIC -- business rules and preventing bad data from entering the system.
# MAGIC -- =======================================================================================
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ==============================================================================
# MAGIC -- HANDLING CORRUPTED DATA AND RECOVERY STRATEGIES
# MAGIC -- ==============================================================================
# MAGIC -- Temporarily drop constraints to allow insertion of bad data for testing purposes
# MAGIC -- ==============================================================================
# MAGIC
# MAGIC ALTER TABLE ${user_schema}.audit_data_quality DROP CONSTRAINT chk_salary;
# MAGIC ALTER TABLE ${user_schema}.audit_data_quality DROP CONSTRAINT chk_department;
# MAGIC
# MAGIC -- ==============================================================================
# MAGIC -- INSERT CORRUPTED DATA FOR TESTING
# MAGIC -- ==============================================================================
# MAGIC -- These values would normally be blocked by the constraints:
# MAGIC --  - Negative salary
# MAGIC --  - Invalid department
# MAGIC -- ==============================================================================
# MAGIC
# MAGIC INSERT INTO ${user_schema}.audit_data_quality VALUES 
# MAGIC ('FFFFF', 'Juan', 'Lopez', -4000, 'Finance'), 
# MAGIC ('GGGGG', 'Elena', 'Perez', 6500, 'Development');
# MAGIC
# MAGIC -- ==============================================================================
# MAGIC -- ATTEMPT TO RE-ADD CONSTRAINTS (This will fail due to invalid data)
# MAGIC -- ==============================================================================
# MAGIC -- The reactivation of constraints will validate existing data and fail
# MAGIC -- ==============================================================================
# MAGIC
# MAGIC ALTER TABLE ${user_schema}.audit_data_quality ADD CONSTRAINT chk_salary CHECK (salary > 0);
# MAGIC ALTER TABLE ${user_schema}.audit_data_quality ADD CONSTRAINT chk_department CHECK (department IN ('Sales', 'IT', 'HR'));
# MAGIC
# MAGIC -- ==============================================================================
# MAGIC -- VERIFY CURRENT DATA IN THE TABLE
# MAGIC -- ==============================================================================
# MAGIC SELECT * FROM ${user_schema}.audit_data_quality;
# MAGIC
# MAGIC -- ==============================================================================
# MAGIC -- WHY THE CONSTRAINT FAILS TO BE RE-ADDED
# MAGIC -- ==============================================================================
# MAGIC -- Delta Lake validates **existing rows** in the table when a new constraint is added.
# MAGIC -- In this case, one or more rows violate the condition `salary > 0`, so the system
# MAGIC -- prevents the constraint from being activated.
# MAGIC --
# MAGIC -- Example error message:
# MAGIC -- [DELTA_NEW_CHECK_CONSTRAINT_VIOLATION] 
# MAGIC -- 1 rows in sandbox.sql_review_<user>.audit_data_quality violate the new CHECK constraint (salary > 0)
# MAGIC -- SQLSTATE: 23512
# MAGIC --
# MAGIC -- To re-enable the constraint, you must either:
# MAGIC --   1. Fix or delete the invalid data manually
# MAGIC --   2. Restore the table to a previous version (if available)
# MAGIC -- ==============================================================================
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ==============================================================================
# MAGIC -- FIXING INVALID RECORDS TO RE-ENABLE DATA QUALITY CONSTRAINTS
# MAGIC -- ==============================================================================
# MAGIC -- If the table contains data that violates constraints (e.g., CHECK conditions),
# MAGIC -- they must be cleaned or corrected before those constraints can be re-applied.
# MAGIC --
# MAGIC -- This section demonstrates:
# MAGIC -- 1. Identifying rows with inconsistent or invalid values
# MAGIC -- 2. Correcting those records with appropriate UPDATE statements
# MAGIC -- 3. Re-enabling the constraints once the data is valid again
# MAGIC -- 4. Verifying that the data now complies with all business rules
# MAGIC --
# MAGIC -- Steps:
# MAGIC -- ------------------------------------------------------------------------------
# MAGIC -- Step 1: Inspect invalid data
# MAGIC SELECT * FROM ${user_schema}.audit_data_quality
# MAGIC WHERE salary < 0 OR department NOT IN ('Sales', 'IT', 'HR');
# MAGIC
# MAGIC -- ------------------------------------------------------------------------------
# MAGIC -- Step 2: Fix negative salary values
# MAGIC UPDATE ${user_schema}.audit_data_quality
# MAGIC SET salary = 5000
# MAGIC WHERE salary < 0;
# MAGIC
# MAGIC -- Step 3: Fix invalid department names
# MAGIC UPDATE ${user_schema}.audit_data_quality
# MAGIC SET department = 'IT'
# MAGIC WHERE department NOT IN ('Sales', 'IT', 'HR');
# MAGIC
# MAGIC -- ------------------------------------------------------------------------------
# MAGIC -- Step 4: Reapply constraints (should now succeed)
# MAGIC ALTER TABLE ${user_schema}.audit_data_quality
# MAGIC ADD CONSTRAINT chk_salary CHECK (salary > 0);
# MAGIC
# MAGIC ALTER TABLE ${user_schema}.audit_data_quality
# MAGIC ADD CONSTRAINT chk_department CHECK (department IN ('Sales', 'IT', 'HR'));
# MAGIC
# MAGIC -- ------------------------------------------------------------------------------
# MAGIC -- Step 5: Verify data is clean
# MAGIC SELECT * FROM ${user_schema}.audit_data_quality;
# MAGIC -- ==============================================================================
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- REVIEW QUESTIONS --
# MAGIC -- The following questions are recommended to reinforce the concepts from this notebook.
# MAGIC
# MAGIC -- 1. What is the purpose of adding CHECK constraints in Delta Lake?
# MAGIC -- 2. What happens when an INSERT operation violates a CHECK constraint?
# MAGIC -- 3. How can you review all changes made to a Delta table over time?
# MAGIC -- 4. What SQL command is used to restore a Delta table to a previous version?
# MAGIC -- 5. How can constraints be safely re-added after they’ve been dropped?
# MAGIC -- 6. Why might a constraint fail to be added after inserting invalid data?
# MAGIC -- 7. What kind of error does Delta Lake return when data violates a constraint?
# MAGIC -- 8. What are some strategies to clean or fix corrupted data?
# MAGIC -- 9. How do you validate that a Delta table complies with business rules after updates?
# MAGIC -- 10. Can constraints be enforced retroactively on existing data?
# MAGIC
# MAGIC -- -----------------------------------------------------------------------
# MAGIC
# MAGIC -- ANSWERS --
# MAGIC
# MAGIC -- 1. To ensure data quality by enforcing rules such as salary > 0 or valid department names.
# MAGIC -- 2. The operation fails with an error (e.g., DELTA_VIOLATE_CONSTRAINT_WITH_VALUES).
# MAGIC -- 3. Using `DESCRIBE HISTORY <table_name>` to access the Delta transaction log.
# MAGIC -- 4. RESTORE TABLE <table_name> TO VERSION AS OF <version_number>;
# MAGIC -- 5. By first cleaning the data so that all rows comply with the constraint logic, then re-adding them.
# MAGIC -- 6. Because Delta validates existing rows when adding a new constraint, and violations will block the change.
# MAGIC -- 7. SQLSTATE: 23512 or 23001, indicating a constraint violation, depending on the error.
# MAGIC -- 8. UPDATE or DELETE the invalid rows, or restore the table to a clean historical version.
# MAGIC -- 9. Use SELECT queries with WHERE filters to detect violations and verify no bad data remains.
# MAGIC -- 10. Yes, but only if all existing data already satisfies the constraint being added.
# MAGIC
