-- Databricks notebook source
/*  
==========================================================================================
DATA RECOVERY WITH TIME TRAVEL AND RESTORE IN DELTA LAKE
==========================================================================================

INTRODUCTION:
- Time Travel in Delta Lake allows you to access previous versions of a table.
- It enables recovering deleted or modified data without needing external backups.
- Use `VERSION AS OF` to query past versions.
- `RESTORE` allows you to revert a table to a previous version.

==========================================================================================
HOW TIME TRAVEL WORKS:
- Delta Lake maintains a change history in the metastore.
- Every operation such as INSERT, UPDATE, DELETE creates a new version of the table.
- You can list all available versions using `DESCRIBE HISTORY`.
- Versions can be accessed by version number (`VERSION AS OF`) or timestamp (`TIMESTAMP AS OF`).

==========================================================================================
HOW RESTORE WORKS:
- `RESTORE TABLE` reverts a Delta table to a previous version.
- All changes after the selected version are discarded.
- Useful for data recovery in case of human errors or accidental data loss.

==========================================================================================
PRACTICAL EXAMPLES:

1. LIST THE CHANGE HISTORY:
   DESCRIBE HISTORY table_name;

2. QUERY DATA FROM AN EARLIER VERSION:
   SELECT * FROM table_name VERSION AS OF 2;

3. QUERY DATA USING A TIMESTAMP:
   SELECT * FROM table_name TIMESTAMP AS OF '2025-03-18 12:00:00';

4. RESTORE THE TABLE TO A PREVIOUS VERSION:
   RESTORE TABLE table_name TO VERSION AS OF 2;

==========================================================================================
USE CASES:
- Recover deleted rows due to user errors.
- Analyze how the data looked at a specific point in time.
- Undo accidental UPDATE or DELETE operations.
- Restore a table after a faulty data load.

==========================================================================================
PRECAUTIONS:
- Time Travel only works while Delta log retention is active.
- `RESTORE` permanently discards changes made after the target version.
- Delta Lake uses cleanup policies (e.g. VACUUM) to manage storage usage.

==========================================================================================
CONCLUSION:
- Time Travel and Restore are essential tools to maintain data integrity.
- They enable recovery without relying on manual backups.
- Proper use improves trust and safety in your data pipelines.

==========================================================================================
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

-- ==============================================================
-- DROP THE TABLE IF IT ALREADY EXISTS
-- ==============================================================

DROP TABLE IF EXISTS ${user_schema}.employees_versioning;

-- ==============================================================
-- CREATE A NEW DELTA TABLE FROM SCRATCH
-- ==============================================================

CREATE TABLE ${user_schema}.employees_versioning (
    ssn STRING,
    first_name STRING,
    last_name STRING,
    salary INT,
    department STRING
) USING DELTA;

-- =======================================================================================
-- INSERT INITIAL RECORDS (VERSION 1)
-- =======================================================================================
INSERT INTO ${user_schema}.employees_versioning VALUES
('AAAAA', 'Amy',   'Reina',   5000, 'Sales'),
('BBBBB', 'Bruce', 'Trillo',  6000, 'IT'),
('CCCCC', 'Carla', 'Rivera',  7000, 'HR');

-- =======================================================================================
-- INSERT NEW RECORDS (VERSION 2)
-- =======================================================================================
INSERT INTO ${user_schema}.employees_versioning VALUES
('DDDDD', 'Danielle', 'Cruz',   5500, 'Sales'),
('EEEEE', 'Pedro',    'Lopez',  6200, 'IT');

-- =======================================================================================
-- UPDATE SALARY FOR ONE EMPLOYEE (VERSION 3)
-- =======================================================================================
UPDATE ${user_schema}.employees_versioning 
SET salary = 7500 
WHERE ssn = 'CCCCC';

-- =======================================================================================
-- DELETE A RECORD (VERSION 4)
-- =======================================================================================
DELETE FROM ${user_schema}.employees_versioning 
WHERE ssn = 'AAAAA';



-- COMMAND ----------

-- =======================================================================================
-- FINAL DATA CHECK
-- =======================================================================================
SELECT * FROM ${user_schema}.employees_versioning;

-- COMMAND ----------

-- =======================================================================================
-- VIEW VERSION HISTORY
-- =======================================================================================
DESCRIBE HISTORY ${user_schema}.employees_versioning;



-- COMMAND ----------

-- =======================================================================================
-- QUERYING PREVIOUS VERSIONS USING TIME TRAVEL
-- =======================================================================================
-- This block demonstrates how to access historical snapshots of a Delta table
-- using `VERSION AS OF`. It's useful to audit past data or recover from accidental changes.
-- =======================================================================================

-- =======================================================================================
-- QUERY VERSION 1, 3, 3, 4... etc 
-- =======================================================================================
-- This query returns the data as it was in version 1 (you can use 1, 2, 3, 4...)
-- =======================================================================================
SELECT * FROM ${user_schema}.employees_versioning VERSION AS OF 1;


-- COMMAND ----------

-- =======================================================================================
-- RESTORE TABLE TO A PREVIOUS VERSION
-- =======================================================================================
-- This command reverts the Delta table to version 1,
-- effectively undoing any updates or deletions performed after that version.
-- Use this with caution: all changes after version 1 will be permanently discarded.
-- =======================================================================================

RESTORE TABLE ${user_schema}.employees_versioning TO VERSION AS OF 1;


-- =======================================================================================
-- POST-RESTORE VALIDATION
-- =======================================================================================
-- After running `RESTORE`, we can inspect the operation metrics in the history log.
-- The output shows:
--   - 1 file was removed (the one created after version 1)
--   - 1 file was restored (from version 1)
--   - Table size adjusted accordingly
-- =======================================================================================

-- You can also restore a Delta table using a TIMESTAMP instead of a version number.
-- This is helpful when you remember *when* the data was correct but not the version ID.
-- If a commit exists at the exact timestamp, Delta will restore that version.
-- If not, Delta will restore the **latest version BEFORE** the specified timestamp.
-- If there’s no version before that time (i.e., table didn't exist yet), it will fail.
-- RESTORE TABLE employee_delta TO TIMESTAMP AS OF '2025-03-25T12:00:00';




-- COMMAND ----------

DESCRIBE HISTORY ${user_schema}.employees_versioning;

-- =======================================================================================
-- RESTORE SUMMARY (Version 7 - From my environment)
-- =======================================================================================
-- The table was successfully reverted to version 1 using `RESTORE TABLE`.
-- Delta Lake internally:
--   - Removed 1 file created after version 1
--   - Restored 1 file from version 1
--   - Reduced total table size from 1556 bytes to 1462 bytes
--   - Updated the table to version 7
--
-- Operation recorded in the Delta transaction log with:
--   operation: RESTORE
--   readVersion: 6
--   numRestoredFiles: 1
--   numRemovedFiles: 1
--   tableSizeAfterRestore: 1462 bytes
-- =======================================================================================



-- COMMAND ----------

-- =======================================================================================
-- VERIFYING TABLE CONTENT AFTER RESTORE
-- =======================================================================================
-- This final SELECT confirms that the table has been successfully reverted to the state
-- it had in version 1.
--
-- The expected output should only include the records present before any UPDATE or DELETE
-- operations were applied in later versions.
--
-- This is a crucial step in validating data recovery via `RESTORE`.
-- =======================================================================================

select * from ${user_schema}.employees_versioning;

-- COMMAND ----------

-- =======================================================================================
-- FORCE VACUUM IMMEDIATELY BY TEMPORARILY LOWERING RETENTION PERIOD
-- =======================================================================================
-- By default, Delta Lake enforces a 7-day minimum retention period to allow Time Travel.
-- The following steps temporarily reduce the retention period to 0 hours to immediately
-- remove obsolete files from previous versions.
-- 
-- NOTE:
-- - This action will permanently remove historical data files.
-- - Time Travel to earlier versions will no longer be possible after this operation.
-- =======================================================================================

-- Step 1: Temporarily disable retention (set to 0 hours)
ALTER TABLE ${user_schema}.employees_versioning 
SET TBLPROPERTIES (
  'delta.logRetentionDuration' = 'interval 0 hours',
  'delta.deletedFileRetentionDuration' = 'interval 0 hours'
);

-- Step 2: Run VACUUM to remove all obsolete files immediately
VACUUM ${user_schema}.employees_versioning RETAIN 0 HOURS;

-- Step 3: Optionally restore the default retention period
ALTER TABLE ${user_schema}.employees_versioning 
SET TBLPROPERTIES (
  'delta.logRetentionDuration' = 'interval 30 days',
  'delta.deletedFileRetentionDuration' = 'interval 7 days'
);


-- COMMAND ----------

-- =======================================================================================
-- CHECK HISTORY AFTER VACUUM
-- =======================================================================================
-- This command shows the table's version history.
-- After running `VACUUM RETAIN 0 HOURS`, older versions may no longer be accessible
-- via Time Travel, but the metadata will still appear in the history log.
-- =======================================================================================

DESCRIBE HISTORY ${user_schema}.employees_versioning;


-- COMMAND ----------

DESCRIBE HISTORY ${user_schema}.employees_versioning;

-- ==============================================================================
-- INTERPRETING `VACUUM` RESULTS FROM DESCRIBE HISTORY ()
-- ==============================================================================

-- These operations confirm that VACUUM was executed and successfully deleted obsolete files:

-- VERSION 9  (VACUUM START)
-- - Triggered with 0-hour retention: {"specifiedRetentionMillis":"0"}
-- - Marked 6 files totaling ~6 KB for deletion: "numFilesToDelete": 6

-- VERSION 10 (VACUUM END)
-- - Status: COMPLETED
-- - Deleted 6 files from 1 directory: "numDeletedFiles": 6, "numVacuumedDirectories": 1

-- VERSION 11 (SET TBLPROPERTIES)
-- - Restored table retention properties to default:
--     delta.logRetentionDuration = interval 30 days
--     delta.deletedFileRetentionDuration = interval 7 days

-- This means Time Travel for older versions is now no longer possible beyond this point,
-- as the physical files were removed. Queries using VERSION AS OF or TIMESTAMP AS OF
-- before version 10 will likely fail unless cached in memory.

-- ==============================================================================


-- COMMAND ----------

-- REVIEW QUESTIONS --
-- The following questions are recommended to reinforce the concepts from this notebook.

-- 1. What is the purpose of the RESTORE TABLE command in Delta Lake?
-- 2. How is RESTORE TABLE different from querying a previous version with VERSION AS OF?
-- 3. What happens to the current data when you run RESTORE TABLE to a previous version?
-- 4. How can you confirm that data has been restored correctly?
-- 5. Can you restore a Delta table to a timestamp instead of a version number?
-- 6. What Delta Lake feature makes data recovery possible?
-- 7. What SQL command helps explore the history of changes made to a Delta table?
-- 8. What operations typically precede a RESTORE use case?
-- 9. Is it possible to undo a RESTORE operation?
-- 10. How does Delta Lake help in auditing and correcting accidental deletes or updates?

-- -----------------------------------------------------------------------

-- ANSWERS --

-- 1. To revert a Delta table back to a previous version and recover lost or corrupted data.
-- 2. RESTORE makes a new version that matches a previous one; VERSION AS OF only queries that older version without modifying the table.
-- 3. It is replaced with the snapshot of data from the restored version, and a new version is created.
-- 4. By comparing the current data with the version that was restored (via SELECT or DESCRIBE HISTORY).
-- 5. Yes, using RESTORE TABLE ... TO TIMESTAMP AS OF 'YYYY-MM-DD HH:MM:SS'.
-- 6. The Delta transaction log, which tracks all operations and enables time travel and restore.
-- 7. DESCRIBE HISTORY <table_name>;
-- 8. Accidental DELETEs, incorrect UPDATEs, or incorrect file overwrites.
-- 9. Yes, by restoring again to a newer version or re-applying updates.
-- 10. Delta provides full version control, enabling rollback, auditability, and precise debugging.
