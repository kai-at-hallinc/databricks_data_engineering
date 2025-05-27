-- Databricks notebook source
/*
================================================================================
F_03 - Querying and Updating Arrays
================================================================================

This notebook is part of the Delta Lake workshop and focuses on working with
ARRAY columns in Delta tables using SQL. Arrays are powerful data structures 
that allow you to represent and manipulate collections of values inside a single column.

This chapter covers how to:

1. Create Delta tables with ARRAY columns
2. Read and access array elements using indexes
3. Apply filters and WHERE conditions using array content
4. Handle out-of-bounds array access safely
5. Perform advanced UPDATE operations on arrays, including:
   - Replacing values by position or content
   - Removing elements
   - Adding elements to the end
   - Combining multiple update strategies with CASE
6. Use Spark SQL functions like:
   - `get()`
   - `transform()`
   - `filter()`
   - `concat()`
   - `array_contains()`
   - `size()`

Special emphasis is placed on understanding the **0-based indexing** of arrays in Spark SQL,
how to avoid common pitfalls (like invalid index errors), and how to perform dynamic updates
based on element position and value.

This notebook includes:

- Several example employees with arrays of certifications
- Clean, commented queries for each pattern
- Errors included on purpose to illustrate safe vs unsafe access
- Updates grouped by purpose: replacing, appending, removing

By the end of this notebook, you'll be comfortable with both reading and modifying
array data in Delta tables using SQL.

================================================================================
IMPORTANT NOTES
================================================================================

- Spark SQL arrays are 0-indexed. The first element is at index 0.
- Attempting to access an index that does not exist (e.g., arr[5] on a 3-element array)
  will result in an error unless you use the `get()` function.
- All UPDATE operations rebuild the entire array, not just one element. This means
  you must use functions like `transform()` or `filter()` to return the full array.

================================================================================
End of Introduction
================================================================================
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

-- This cell creates a Delta table named `employee_array` to demonstrate how to work with ARRAY columns in Delta Lake.
-- The table includes a new column `certifications`, which is an array of strings representing each employee's certifications.
-- We'll use this table to practice SELECT, WHERE, and UPDATE operations involving arrays:
--    - Selecting specific elements (e.g., the first certification)
--    - Filtering rows based on array content or position
--    - Updating specific positions inside the array
-- Arrays in Spark SQL are 0-indexed: the first element is at position 0.

-- Use user-specific schema
USE ${user_schema};

-- Temporary view with array data
CREATE OR REPLACE TEMP VIEW tmp_view_employee_array AS
SELECT 
  'AAAAA' AS ssn, 
  'Amy' AS first_name, 
  'Reina' AS last_name, 
  5000 AS salary, 
  CAST('2025-01-01' AS DATE) AS entry_date,
  ARRAY('azure', 'databricks', 'mlflow', 'spark') AS certifications
UNION ALL
SELECT 
  'BBBBB', 'Bruce', 'Trillo', 6000, CAST('2025-02-01' AS DATE), 
  ARRAY('aws', 'python', 'sql')
UNION ALL
SELECT 
  'CCCCC', 'Carla', 'Rivera', 7000, CAST('2025-03-01' AS DATE), 
  ARRAY('databricks', 'sql')
UNION ALL
SELECT 
  'DDDDD', 'Daniel', 'Paz', 5500, CAST('2025-03-15' AS DATE), 
  ARRAY('azure', 'excel');

-- Create the Delta table
CREATE OR REPLACE TABLE employee_array AS
SELECT * FROM tmp_view_employee_array;

-- Check the table
SELECT * FROM employee_array;




-- COMMAND ----------

-- Use user-specific schema
USE ${user_schema};

-- This query selects specific elements from the array column.
-- Arrays in Spark SQL are 0-based: the first element is at index 0.
-- We retrieve the first and second certification from the `certifications` array.

SELECT 
  ssn,
  certifications,
  certifications[0] AS first_cert,
  certifications[1] AS second_cert
FROM employee_array;


-- COMMAND ----------

-- This query filters rows where the second element (index 1) in the array equals 'python'.
-- If an array does not contain an element at the specified index, the result will be NULL and the row will be excluded.

SELECT *
FROM employee_array
WHERE certifications[1] = 'python';


-- COMMAND ----------

-- This query retrieves specific elements and applies a filter at the same time.
-- We return the first and second certifications only for employees whose first certification is 'azure'.

SELECT 
  ssn,
  certifications[0] AS first_cert,
  certifications[1] AS second_cert
FROM employee_array
WHERE certifications[0] = 'azure';


-- COMMAND ----------

-- MAGIC %python
-- MAGIC # This block tries to access the fourth element of the array using direct indexing.
-- MAGIC # If the index is out of bounds, it catches the error and prints a custom message.
-- MAGIC # We used python instead of sql because sql does not support try/catch. 
-- MAGIC
-- MAGIC try:
-- MAGIC     spark.sql("""
-- MAGIC         SELECT 
-- MAGIC           ssn,
-- MAGIC           certifications,
-- MAGIC           certifications[3] AS fourth_cert
-- MAGIC         FROM employee_array
-- MAGIC         WHERE ssn = 'CCCCC'
-- MAGIC     """).show(truncate=False)
-- MAGIC except Exception as e:
-- MAGIC     print("Error occurred while accessing certifications[3]:")
-- MAGIC     # print(e)
-- MAGIC     print("\nHint: Use get(certifications, 3) instead to avoid index errors.")
-- MAGIC

-- COMMAND ----------

-- This version uses the get() function to safely access the array.
-- Like Python, get() uses 0-based indexing and returns NULL if the index does not exist.

SELECT 
  ssn,
  certifications,
  get(certifications, 0) AS first_cert,
  get(certifications, 3) AS fourth_cert  -- Index 3 = 4th element, returns NULL if out of range
FROM employee_array;


-- COMMAND ----------

-- This UPDATE modifies the third element (index 2) of the `certifications` array for a specific employee.
-- The `transform()` function is used to rebuild the array by applying a conditional update to each element.
-- Arrays are 0-based, so index = 2 refers to the third element.
-- In this example, we replace 'mlflow' with 'tensorflow' at index 2 for employee AAAAA.
-- (see next cell)

select certifications from employee_array
WHERE ssn = 'AAAAA';


-- COMMAND ----------

-- This UPDATE modifies the third element (index 2) of the `certifications` array for a specific employee.
-- The `transform()` function is used to rebuild the array by applying a conditional update to each element.
-- Arrays are 0-based, so index = 2 refers to the third element.
-- In this example, we replace 'mlflow' with 'tensorflow' at index 2 for employee AAAAA.

UPDATE employee_array
SET certifications = transform(certifications, (x, i) -> 
  CASE 
    WHEN i = 2 THEN 'tensorflow'  -- Replace the third element
    ELSE x
  END
)
WHERE ssn = 'AAAAA';

SELECT certifications FROM employee_array
WHERE ssn = 'AAAAA';


-- COMMAND ----------

-- This UPDATE modifies two positions in the array for a specific employee (AAAAA).
-- It replaces the second (index 1) and third (index 2) certifications.

UPDATE employee_array
SET certifications = transform(certifications, (x, i) -> 
  CASE 
    WHEN i = 1 THEN 'unity'        -- replace 2nd element
    WHEN i = 2 THEN 'kafka'        -- replace 3rd element
    ELSE x
  END
)
WHERE ssn = 'AAAAA';

SELECT certifications FROM employee_array
WHERE ssn = 'AAAAA';


-- COMMAND ----------

-- This UPDATE appends a new certification ('bigquery') at the end of the array using concat().

UPDATE employee_array
SET certifications = concat(certifications, array('bigquery'))
WHERE ssn = 'BBBBB';

SELECT certifications FROM employee_array
WHERE ssn = 'BBBBB';


-- COMMAND ----------

-- This UPDATE removes 'sql' from the certifications array using filter().

UPDATE employee_array
SET certifications = filter(certifications, x -> x != 'sql')
WHERE ssn = 'BBBBB';


SELECT certifications FROM employee_array
WHERE ssn = 'BBBBB';

-- COMMAND ----------

SELECT certifications FROM employee_array
WHERE ssn = 'CCCCC';

-- COMMAND ----------

-- This UPDATE removes the second element (index 1) from the array by filtering based on index.

UPDATE employee_array
SET certifications = filter(certifications, (x, i) -> i != 1)
WHERE ssn = 'CCCCC';

SELECT certifications FROM employee_array
WHERE ssn = 'CCCCC';


-- COMMAND ----------

-- This UPDATE attempts to remove the second element (index 1) from the `certifications` array for employee CCCCC.
-- However, if a previous operation already removed that element (e.g., in a prior UPDATE),
-- this UPDATE will have no effect because there is no element at index 1 to remove.
-- This demonstrates that array updates are state-dependent, and applying the same logic multiple times
-- may yield no changes if the condition no longer applies.

UPDATE employee_array
SET certifications = filter(certifications, (x, i) -> i != 1)
WHERE ssn = 'CCCCC';

-- Confirm the result
SELECT certifications FROM employee_array
WHERE ssn = 'CCCCC';


-- COMMAND ----------

-- Add two new employees with rich certification arrays for advanced testing

INSERT INTO employee_array
VALUES 
('EEEEE', 'Elena', 'Cortez', 8000, DATE '2025-04-01', 
 ARRAY('azure', 'databricks', 'mlflow', 'pandas', 'spark', 'kafka', 'airflow')),
('FFFFF', 'Flavio', 'Jimenez', 8200, DATE '2025-04-05', 
 ARRAY('aws', 'hadoop', 'databricks', 'spark', 'python', 'sql', 'tableau'));

SELECT ssn, certifications 
FROM employee_array
WHERE ssn IN ('EEEEE', 'FFFFF');


-- COMMAND ----------

-- This UPDATE applies different transformations to each employee using a CASE inside transform().
-- For EEEEE (Elena): replace 'mlflow' with 'tensorflow'
-- For FFFFF (Flavio): replace 'sql' with 'rust'

UPDATE employee_array
SET certifications = transform(certifications, x ->
  CASE 
    WHEN ssn = 'EEEEE' AND x = 'mlflow' THEN 'tensorflow'
    WHEN ssn = 'FFFFF' AND x = 'sql' THEN 'rust'
    ELSE x
  END
)
WHERE ssn IN ('EEEEE', 'FFFFF');

SELECT ssn, certifications 
FROM employee_array
WHERE ssn IN ('EEEEE', 'FFFFF');


-- COMMAND ----------

-- This query counts the number of certifications each employee has
-- using the size() function on the array.

SELECT 
  ssn,
  size(certifications) AS cert_count,
  certifications
FROM employee_array
WHERE ssn IN ('EEEEE', 'FFFFF');


-- COMMAND ----------

-- This query filters employees who have 'spark' as one of their certifications
-- using the array_contains() function.

SELECT 
  ssn,
  certifications
FROM employee_array
WHERE array_contains(certifications, 'spark')
  AND ssn IN ('EEEEE', 'FFFFF');


-- COMMAND ----------

-- This UPDATE removes the certification 'airflow' if present in the array.
-- Applies only to Elena (EEEEE) in this case.

UPDATE employee_array
SET certifications = filter(certifications, x -> x != 'airflow')
WHERE ssn = 'EEEEE';

-- Verify the result
SELECT ssn, certifications 
FROM employee_array
WHERE ssn = 'EEEEE';


-- COMMAND ----------

-- This UPDATE removes the third element (index 2) from the array.
-- Applies to both employees.

UPDATE employee_array
SET certifications = filter(certifications, (x, i) -> i != 2)
WHERE ssn IN ('EEEEE', 'FFFFF');

-- Verify the result
SELECT ssn, certifications 
FROM employee_array
WHERE ssn IN ('EEEEE', 'FFFFF');


-- COMMAND ----------

-- This UPDATE appends a new certification 'delta' to the end of the array
-- for both Elena and Flavio.

UPDATE employee_array
SET certifications = concat(certifications, array('delta'))
WHERE ssn IN ('EEEEE', 'FFFFF');

-- Verify the result
SELECT ssn, certifications 
FROM employee_array
WHERE ssn IN ('EEEEE', 'FFFFF');


-- COMMAND ----------

-- This UPDATE replaces values in the array based on content:
-- 'databricks' becomes 'dbx'
-- 'python' becomes 'py'
-- Applies to both employees.

UPDATE employee_array
SET certifications = transform(certifications, x -> 
  CASE 
    WHEN x = 'databricks' THEN 'dbx'
    WHEN x = 'python' THEN 'py'
    ELSE x
  END
)
WHERE ssn IN ('EEEEE', 'FFFFF');

-- Verify the result
SELECT ssn, certifications 
FROM employee_array
WHERE ssn IN ('EEEEE', 'FFFFF');


-- COMMAND ----------

-- REVIEW QUESTIONS --
-- The following questions are recommended to reinforce the concepts from this notebook.

-- 1. How can you access the third element in an array safely?
-- 2. What is the difference between array[i] and get(array, i)?
-- 3. What happens if you try to access an out-of-bounds index using array[i]?
-- 4. How can you filter rows based on a value in a specific array position?
-- 5. How do you replace an element inside an array at a given index?
-- 6. What is the purpose of the transform() function when working with arrays?
-- 7. How do you remove an element from an array by value?
-- 8. How do you remove an element from an array by position?
-- 9. How can you append an item to the end of an array?
-- 10. What is the role of posexplode() when combined with array updates?

-- -----------------------------------------------------------------------

-- ANSWERS --

-- 1. Use get(array, 2), since get() returns NULL if the index is out of bounds (arrays are 0-based).
-- 2. array[i] throws an error if the index doesn't exist; get(array, i) returns NULL safely.
-- 3. You get an error like INVALID_ARRAY_INDEX unless you use get() to protect the access.
-- 4. Use WHERE get(array_col, position) = 'value' to filter based on array content.
-- 5. Use transform(array, (x, i) -> CASE WHEN i = target_index THEN 'new_value' ELSE x END).
-- 6. It allows rebuilding the array element-by-element, optionally with conditional replacement logic.
-- 7. Use filter(array, x -> x != 'value_to_remove').
-- 8. Use filter(array, (x, i) -> i != index_to_remove).
-- 9. Use concat(array, array('new_element')).
-- 10. It helps track the position of each element while exploding or transforming arrays.
