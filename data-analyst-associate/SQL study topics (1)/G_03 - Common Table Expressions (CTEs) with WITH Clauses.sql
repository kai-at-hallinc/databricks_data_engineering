-- Databricks notebook source
/*
=======================================================================================
G_03 - Common Table Expressions (CTEs) with WITH Clauses
=======================================================================================

This notebook introduces **Common Table Expressions (CTEs)** in SQL,
a powerful technique to simplify complex queries and improve readability using `WITH` clauses.

CTEs are especially useful when:
  - You need to break down a query into logical steps
  - You want to avoid repeating subqueries
  - You are chaining transformations
  - You want to make your SQL easier to debug and maintain

---------------------------------------------------------------------------------------
What is a CTE?
---------------------------------------------------------------------------------------

A Common Table Expression (CTE) is a temporary, named result set defined using the `WITH` keyword.
It can be referenced later in the same query as if it were a table or view.

CTEs are:
  - Scoped to the statement where they are defined
  - Not persisted (they live only during the execution of that query)
  - Reusable if defined once, which improves performance and clarity

---------------------------------------------------------------------------------------
Structure
---------------------------------------------------------------------------------------

This notebook will cover:

✓ Basic CTE syntax and usage  
✓ Using CTEs to replace subqueries  
✓ Chaining multiple CTEs  
✓ Filtering and aggregating CTE outputs  
✓ Optional: recursion preview (if needed)  
✓ Final review questions

---------------------------------------------------------------------------------------
Dataset
---------------------------------------------------------------------------------------

We’ll use the existing `employee_window` table from G_02, which includes:

  - SSN
  - First name
  - City
  - Department
  - Salary

This familiar dataset will help us focus on mastering the CTE syntax and logic flow.

---------------------------------------------------------------------------------------
Tip
---------------------------------------------------------------------------------------

Think of a CTE as a way to:
  - "Name" the result of a subquery
  - Reuse it multiple times
  - Read your SQL top-down, like steps in a pipeline

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

-- CTEs require a working dataset, so we recreate the same employee_window table
-- used in G_02. This makes the notebook self-contained and ready to run independently.

USE ${user_schema};

CREATE OR REPLACE TABLE employee_window AS
SELECT * FROM VALUES
  ('AAAAA', 'Alice',   'New York',     'Sales',     7000),
  ('BBBBB', 'Bob',     'New York',     'Sales',     7000),
  ('CCCCC', 'Carla',   'Chicago',      'Sales',     7100),
  ('DDDDD', 'Dan',     'Chicago',      'HR',        7300),
  ('EEEEE', 'Eva',     'Kansas City',  'HR',        7200),
  ('FFFFF', 'Frank',   'San Diego',    'IT',        8000),
  ('GGGGG', 'Gina',    'San Diego',    'IT',        7800),
  ('HHHHH', 'Hugo',    'Chicago',      'Marketing', 6700),
  ('IIIII', 'Iris',    'New York',     'Marketing', 6600),
  ('JJJJJ', 'James',   'Chicago',      'Finance',   7400)
AS t(ssn, first_name, city, department, salary);


-- COMMAND ----------

-- Department info table
-- -----------------------
-- This table contains basic information about each department,
-- including its physical address (for join purposes).

CREATE OR REPLACE TABLE department_info AS
SELECT * FROM VALUES
  ('Sales',      '100 Park Ave, New York'),
  ('HR',         '200 Main St, Chicago'),
  ('IT',         '300 Tech Dr, San Diego'),
  ('Marketing',  '400 Market Rd, New York'),
  ('Finance',    '500 Wall St, Chicago')
AS t(department, address);


-- COMMAND ----------

-- Chaining Multiple CTEs
-- ------------------------
-- You can define more than one CTE by separating them with commas.
-- Each CTE can build on the previous ones, like steps in a query pipeline.
--
-- Note: The same result could be achieved with a single WHERE clause using AND.
--       We're using multiple CTEs here purely to illustrate how to structure 
--       and compose queries in a clear, modular way.

-- In this example:
--   1. The first CTE filters Sales employees
--   2. The second CTE finds those earning more than 6900
--   3. The final SELECT presents the filtered result

WITH sales_employees AS (
  SELECT first_name, city, salary
  FROM employee_window
  WHERE department = 'Sales'
),
high_earners AS (
  SELECT first_name, city, salary
  FROM sales_employees
  WHERE salary > 6900
)

SELECT 
  first_name,
  city,
  salary
FROM high_earners;


-- COMMAND ----------

-- CTE with JOIN Example
-- -----------------------
-- In this example, we want to show employee info along with the physical
-- address of their department — but only for employees earning more than 7000.

-- Step 1: Use a CTE to pre-filter employees with salary > 7000
-- Step 2: Join the CTE with the department_info table on department
-- Step 3: Select fields from both tables in the final output

WITH high_earning_employees AS (
  SELECT first_name, department, city, salary
  FROM employee_window
  WHERE salary > 7000
)

SELECT 
  e.first_name,
  e.department,
  e.city,
  e.salary,
  d.address
FROM high_earning_employees e
JOIN department_info d
  ON e.department = d.department
ORDER BY e.salary DESC;


-- COMMAND ----------

-- Creating a view that internally uses a CTE
-- -------------------------------------------
-- Even though we could write this as a single SELECT, we define a CTE
-- inside the view definition to demonstrate how modular logic can be reused.

-- The CTE filters high-earning employees, and the final SELECT joins
-- that result with the department_info table to add the address.

CREATE OR REPLACE VIEW v_high_earners_with_address AS
WITH high_earners AS (
  SELECT first_name, department, city, salary
  FROM employee_window
  WHERE salary > 7000
)
SELECT 
  e.first_name,
  e.department,
  e.city,
  e.salary,
  d.address
FROM high_earners e
JOIN department_info d
  ON e.department = d.department;


-- COMMAND ----------

-- Testing the view
SELECT * FROM v_high_earners_with_address
ORDER BY salary DESC;

-- COMMAND ----------

-- Using LAG() and LEAD() over a view with a CTE
-- -----------------------------------------------
-- This example demonstrates how window functions can operate over a view
-- whose logic is built using a CTE.
--
-- We use:
--   - LAG(first_name): shows the previous employee in alphabetical order
--   - LEAD(first_name): shows the next employee in alphabetical order
--
-- This is useful for navigation, comparisons, or neighbor-based logic.

SELECT 
  first_name,
  department,
  city,
  salary,
  address,
  LAG(first_name) OVER (
    ORDER BY first_name
  ) AS previous_first_name,
  LEAD(first_name) OVER (
    ORDER BY first_name
  ) AS next_first_name
FROM v_high_earners_with_address;


-- COMMAND ----------

-- REVIEW QUESTIONS --
-- --------------------

-- 1. What does a Common Table Expression (CTE) do?
--     a) Creates a permanent table
--     b) Creates a temporary view across notebooks
--     c) Defines a named temporary result set scoped to a single query
--     d) Stores results to disk for reuse

-- 2. Which keyword is used to define a CTE?
--     a) TEMP TABLE
--     b) DEFINE
--     c) WITH
--     d) LET

-- 3. True or False:
--     You can define multiple CTEs in the same query by separating them with commas.

-- 4. Why might using a CTE before a JOIN improve performance?
--     a) It materializes the join in memory
--     b) It avoids the join entirely
--     c) It allows filtering to happen before the join, reducing data volume
--     d) It parallelizes the query

-- 5. Can a CTE be used inside a view?
--     a) No, only in standalone queries
--     b) Yes, it becomes part of the view’s logic
--     c) Only in temporary views
--     d) Only with UNION ALL

-- 6. What does LAG(first_name) OVER (ORDER BY first_name) return?
--     a) The next name alphabetically
--     b) The first value in the table
--     c) The previous name in alphabetical order
--     d) The department of the previous row

-- ----------------------------------------------------
-- Answers:
-- 1. c) Defines a named temporary result set scoped to a single query  
-- 2. c) WITH  
-- 3. True  
-- 4. c) It allows filtering to happen before the join, reducing data volume  
-- 5. b) Yes, it becomes part of the view’s logic  
-- 6. c) The previous name in alphabetical order
