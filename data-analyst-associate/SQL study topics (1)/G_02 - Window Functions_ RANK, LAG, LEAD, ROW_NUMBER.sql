-- Databricks notebook source
/*
=======================================================================================
G_02 - Window Functions: RANK, LAG, LEAD, ROW_NUMBER
=======================================================================================

This notebook introduces core **window functions** in SQL—powerful tools that allow you
to perform calculations across rows related to the current row, without collapsing them
into a single group (as GROUP BY does).

Window functions are essential for analytics, ranking, comparisons between rows, and
many reporting scenarios.

---------------------------------------------------------------------------------------
Dataset
---------------------------------------------------------------------------------------

We’ll use the same `employee_analytics` table introduced in G_01. It includes:

    - SSN (synthetic, e.g., 'AAAAA')
    - First name
    - Department
    - Location
    - Salary
    - Entry date

This dataset lets us explore window functions across logical partitions (e.g., by department).

---------------------------------------------------------------------------------------
Covered Functions
---------------------------------------------------------------------------------------

✓ RANK()         - Assigns ranking with gaps for ties  
✓ DENSE_RANK()   - Like RANK, but without gaps  
✓ ROW_NUMBER()   - Assigns a unique row number within each partition  
✓ LAG()          - Gets the previous row's value  
✓ LEAD()         - Gets the next row's value  
✓ PARTITION BY   - Splits the dataset for independent calculations  
✓ ORDER BY       - Defines the row order inside each partition

---------------------------------------------------------------------------------------
Structure
---------------------------------------------------------------------------------------

Each function will be introduced with:

1. An explanation of the use case
2. A query that demonstrates it in context
3. Examples of combining multiple window functions
4. Edge case handling (e.g., nulls, gaps, first row)
5. Review questions at the end

---------------------------------------------------------------------------------------
Note
---------------------------------------------------------------------------------------

Window functions are extremely common in both SQL-based analytics and in tools like
Power BI, Tableau, and Excel PivotTables with advanced calculations.

Mastering them is critical for data analysts, engineers, and anyone writing complex queries.

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

-- WINDOW FUNCTIONS: RANK, ROW_NUMBER, LAG, LEAD
-- ---------------------------------------------
-- Let's create a new table `employee_window` that we will use to demonstrate
-- core window functions such as RANK(), ROW_NUMBER(), LAG(), and LEAD().

-- The structure is similar to what we've used before but tailored for ranking scenarios.

-- Fields:
--   - ssn: synthetic identifier (e.g., 'AAAAA')
--   - first_name: matches the initial of ssn
--   - city: city of employment
--   - department: business unit
--   - salary: base salary

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

-- ROW_NUMBER() Example
-- ---------------------
-- ROW_NUMBER() assigns a unique sequential number to each row **within a partition**,
-- ordered by a specific column.
--
-- It's commonly used when you want to:
--   - Number rows inside a group (like department)
--   - Select "top N" per group
--   - Deduplicate rows (e.g., keep latest)
--
-- In this example, we assign a row number to each employee **within their department**,
-- ordered by salary descending.
-- See what happens with row_num in "Bob - Sales"

SELECT 
  first_name,
  department,
  salary,
  ROW_NUMBER() OVER (
    PARTITION BY department -- <-- partition by department, we need to use a window function that "resets" the row number for each department
    ORDER BY salary DESC -- <-- order by salary descending
  
  ) AS row_num
FROM employee_window;


-- COMMAND ----------

-- RANK() Example
-- ----------------
-- RANK() assigns a ranking number to each row **within a partition**, 
-- ordered by a specific column (e.g., salary). 
--
-- Unlike ROW_NUMBER(), RANK() gives **the same number to tied values**, 
-- and skips the next rank(s) accordingly (i.e., it leaves gaps).
--
-- Use it when you want to:
--   - Identify top performers (including ties)
--   - Generate ordered lists with proper ranks
--
-- In this example, we rank employees within each department by salary descending.
-- If two employees have the same salary, they’ll share the same rank.
-- See what happens with row_num in "Bob - Sales"

SELECT 
  first_name,
  department,
  salary,
  RANK() OVER (
    PARTITION BY department -- partition by department
    ORDER BY salary DESC    -- order salaries from highest to lowest
  ) AS dept_rank
FROM employee_window;


-- COMMAND ----------

-- DENSE_RANK() Example
-- -----------------------
-- DENSE_RANK() works like RANK(), but without leaving gaps.
-- If multiple rows tie, they get the same rank—but the next rank increases by 1.
--
-- Use DENSE_RANK() when:
--   - You want to rank tied values the same
--   - But prefer not to skip numbers (e.g., for reports or scoring)
--
-- In this example:
--   - Carla earns 7100 → rank 1
--   - Alice and Bob both earn 7000 → both get rank 2 (no gap)
-- 
-- This makes DENSE_RANK() useful for grouping ties without leaving holes in ranking.

SELECT 
  first_name,
  department,
  salary,
  DENSE_RANK() OVER (
    PARTITION BY department -- reset rank for each department
    ORDER BY salary DESC    -- highest salary first
  ) AS dense_rank
FROM employee_window;


-- COMMAND ----------

-- LAG() Example
-- ----------------
-- LAG() allows you to access the value from a **previous row** within the same partition.
-- It’s commonly used to:
--   - Compare current vs. previous values (e.g., salary changes)
--   - Detect trends over time
--   - Create "diff" columns (current - previous)

-- In this example:
--   - We partition by department
--   - We order by salary descending
--   - We use LAG() to get the previous salary within the same department

SELECT 
  first_name,
  department,
  salary,
  LAG(salary) OVER (
    PARTITION BY department
    ORDER BY salary DESC
  ) AS previous_salary
FROM employee_window;


-- COMMAND ----------

-- Salary Difference with LAG()
-- -------------------------------
-- This version uses LAG() to calculate the difference between the current salary
-- and the previous one within each department.
--
-- Use cases:
--   - Detect pay gaps
--   - Calculate salary progression
--   - Identify jumps or drops in ordered data

SELECT 
  first_name,
  department,
  salary,
  LAG(salary) OVER (
    PARTITION BY department
    ORDER BY salary DESC
  ) AS previous_salary,

  LAG(salary) OVER (
    PARTITION BY department
    ORDER BY salary DESC
  ) - salary AS salary_diff

FROM employee_window;


-- COMMAND ----------

-- LEAD() Example
-- ----------------
-- LEAD() returns the value from the **next row** in the window partition.
--
-- It’s useful to:
--   - Compare current row with the next one
--   - Analyze future values or trends
--   - Detect upcoming changes (e.g., next salary tier)

-- In this example:
--   - We partition by department
--   - We order by salary descending
--   - LEAD() shows the next salary below the current one in the ranking

SELECT 
  first_name,
  department,
  salary,
  LEAD(salary) OVER (
    PARTITION BY department
    ORDER BY salary DESC
  ) AS next_salary
FROM employee_window;


-- COMMAND ----------

-- Salary Gap with LEAD()
-- ------------------------
-- This version uses LEAD() to calculate the difference between the current salary
-- and the next one (lower in rank) within the same department.
--
-- Use cases:
--   - Detect how much lower the next salary is
--   - Analyze pay step-downs within a team
--   - Spot sharp salary gaps

SELECT 
  first_name,
  department,
  salary,
  LEAD(salary) OVER (
    PARTITION BY department
    ORDER BY salary DESC
  ) AS next_salary,

  salary - LEAD(salary) OVER (
    PARTITION BY department
    ORDER BY salary DESC
  ) AS salary_gap

FROM employee_window;


-- COMMAND ----------

-- REVIEW QUESTIONS --
-- --------------------

-- 1. What is the difference between ROW_NUMBER() and RANK() when two rows have the same value?
--     a) ROW_NUMBER() assigns the same number, RANK() assigns different ones
--     b) Both behave the same
--     c) ROW_NUMBER() assigns unique numbers, RANK() assigns the same number and skips the next
--     d) RANK() does not support ties

-- 2. Which function can be used to access the next row’s value?
--     a) LAG()
--     b) RANK()
--     c) LEAD()
--     d) ROW_NUMBER()

-- 3. True or False:
--     DENSE_RANK() assigns the same number to ties and does not leave gaps in ranking.

-- 4. What does PARTITION BY do in a window function?
--     a) Sorts all rows globally
--     b) Splits the dataset into groups before applying the function
--     c) Filters rows by condition
--     d) Joins partitions

-- 5. If you want to calculate the salary difference between an employee and the one ranked below, which function should you use?
--     a) RANK()
--     b) LEAD()
--     c) DENSE_RANK()
--     d) ROW_NUMBER()

-- ----------------------------------------------------
-- Answers:
-- 1. c) ROW_NUMBER() assigns unique numbers, RANK() assigns the same number and skips the next
-- 2. c) LEAD()
-- 3. True
-- 4. b) Splits the dataset into groups before applying the function
-- 5. b) LEAD()
