-- Databricks notebook source
/*
=======================================================================================
G_01 - Grouping Extensions: ROLLUP, CUBE, and GROUPING SETS, Views
=======================================================================================

This notebook introduces advanced SQL grouping techniques using three powerful extensions:
    - ROLLUP
    - CUBE
    - GROUPING SETS

These operations allow you to go beyond standard GROUP BY queries by calculating subtotals,
cross-tab aggregations, and highly customized summaries—all without writing multiple queries.

---------------------------------------------------------------------------------------
Dataset
---------------------------------------------------------------------------------------

We will use a simple employee dataset (`employee_analytics`) with 10 records. Each row contains:
    - A synthetic SSN (e.g., 'AAAAA')
    - First name
    - Department
    - Location (city)
    - Salary
    - Entry date

This structure allows us to explore how different grouping strategies behave across departments
and locations, using both individual queries and reusable views.

---------------------------------------------------------------------------------------
Covered Concepts
---------------------------------------------------------------------------------------

✓ ROLLUP: Generates hierarchical subtotals  
✓ CUBE: Generates all possible combinations of groupings  
✓ GROUPING SETS: Lets you define exactly which groupings to include  
✓ GROUPING_ID(): Identifies which levels of grouping are active  
✓ Views: Reusable virtual tables that encapsulate complex queries  
✓ Filtering and querying views for subtotals or specific groups  

---------------------------------------------------------------------------------------
Structure
---------------------------------------------------------------------------------------

Each section contains:

1. A clear explanation of the technique
2. A query that demonstrates the behavior
3. A view that encapsulates the logic
4. Sample filters to explore the results
5. A final summary that compares the three methods
6. Review questions to reinforce learning

---------------------------------------------------------------------------------------
Note
---------------------------------------------------------------------------------------

These grouping techniques are common in reporting and dashboarding systems.
Understanding them is critical for building aggregated views of data for business intelligence
or analytics use cases.

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

-- GROUPING EXTENSIONS: ROLLUP, CUBE, and GROUPING SETS
-- -----------------------------------------------------
-- This notebook introduces advanced SQL grouping techniques using ROLLUP, CUBE, and GROUPING SETS.
-- These extensions allow us to generate subtotals, grand totals, and multidimensional aggregations
-- without writing multiple GROUP BY queries manually.

-- In this first example, we will create a base table called `employee_analytics` that will be reused 
-- throughout this notebook and other notebooks in section G.

-- The table contains employees from multiple departments, working in different locations,
-- with their salaries and entry dates. This dataset is perfect for exploring grouping behavior.

-- We will later use:
--   - ROLLUP to calculate subtotals by department
--   - CUBE to calculate all combinations of groupings
--   - GROUPING SETS to define specific groupings manually

-- Let's create the base table.

USE ${user_schema};

CREATE OR REPLACE TABLE employee_analytics AS
SELECT * FROM VALUES
  ('AAAAA', 'Alice',   'Sales',     'Philadelphia', 7000, DATE('2022-01-10')),
  ('BBBBB', 'Bob',     'Sales',     'Philadelphia', 6900, DATE('2022-03-12')),
  ('CCCCC', 'Carla',   'Sales',     'Miami',        7100, DATE('2023-02-01')),
  ('DDDDD', 'Dan',     'HR',        'Kansas City',  7300, DATE('2023-03-15')),
  ('EEEEE', 'Eva',     'HR',        'Chicago',      7200, DATE('2021-11-05')),
  ('FFFFF', 'Frank',   'IT',        'Oklahoma',     8000, DATE('2022-06-01')),
  ('GGGGG', 'Gina',    'IT',        'San Diego',    7800, DATE('2021-10-20')),
  ('HHHHH', 'Hugo',    'Marketing', 'Chicago',      6700, DATE('2022-08-15')),
  ('IIIII', 'Iris',    'Marketing', 'New York',     6600, DATE('2022-09-10')),
  ('JJJJJ', 'James',   'Finance',   'Chicago',      7400, DATE('2023-01-20'))
AS t(ssn, first_name, department, location, salary, entry_date);


-- COMMAND ----------

-- ROLLUP Example with Multiple Aggregations
-- ------------------------------------------
-- This example shows how ROLLUP behaves with different aggregate functions:
--   - SUM(salary): total payroll by group
--   - AVG(salary): average salary per group
--   - MAX(salary): highest salary in group
--   - MIN(salary): lowest salary in group

-- GROUPING_ID(department, location) helps identify subtotal and grand total rows.
-- See https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-syntax-qry-select-groupby

SELECT 
  department, 
  location, 
  SUM(salary)      AS total_salary,
  ROUND(AVG(salary), 2) AS avg_salary,
  MAX(salary)      AS max_salary,
  MIN(salary)      AS min_salary,
  GROUPING_ID(department, location) AS grouping_level
FROM employee_analytics
GROUP BY ROLLUP(department, location)
ORDER BY department, location;


-- COMMAND ----------

-- Creating a View with ROLLUP Aggregation
-- ----------------------------------------

-- A VIEW is a named, stored query. It behaves like a virtual table.
-- Instead of writing a complex query every time, we can wrap it in a VIEW
-- and query the view directly as if it were a table.

-- This view summarizes total, average, min and max salaries per department and location,
-- using ROLLUP to generate subtotals and a grand total.
-- It also includes GROUPING_ID() to identify the aggregation level.

-- We can later filter this view just like any other query.

CREATE OR REPLACE VIEW v_rollup_salary_summary AS
SELECT 
  department, 
  location, 
  SUM(salary)          AS total_salary,
  ROUND(AVG(salary), 2) AS avg_salary,
  MAX(salary)          AS max_salary,
  MIN(salary)          AS min_salary,
  GROUPING_ID(department, location) AS grouping_level
FROM employee_analytics
GROUP BY ROLLUP(department, location);


-- COMMAND ----------

-- Querying the view we just created
-- ----------------------------------
-- The view `v_rollup_salary_summary` behaves like a virtual table.
-- You can query it directly, filter it, or join it with other tables.

SELECT * 
FROM v_rollup_salary_summary
ORDER BY department, location;


-- COMMAND ----------

-- Filtering only subtotal rows (GROUPING_ID = 1)
-- -----------------------------------------------
-- GROUPING_ID = 1 means the row is a subtotal by department,
-- where location is NULL, but department is still grouped.

SELECT * 
FROM v_rollup_salary_summary
WHERE grouping_level = 1
ORDER BY department;


-- COMMAND ----------

-- Filtering results to show only the Sales department
-- -----------------------------------------------------
-- Views can be filtered like regular tables.
-- This query shows all data related to the Sales department,
-- including subtotals (location = NULL if present).

SELECT * 
FROM v_rollup_salary_summary
WHERE department = 'Sales';


-- COMMAND ----------

-- Filtering only the grand total row (GROUPING_ID = 3)
-- -----------------------------------------------------
-- GROUPING_ID = 3 means both department and location are NULL,
-- which represents the grand total across the entire table.

SELECT * 
FROM v_rollup_salary_summary
WHERE grouping_level = 3;



-- COMMAND ----------

-- CUBE Example: department × location
-- -------------------------------------
-- CUBE generates all possible combinations of groupings between the selected columns.
-- In this case, we get:
--   - A row for each department + location (normal grouping)
--   - Subtotals per department (location = NULL)
--   - Subtotals per location (department = NULL)
--   - A grand total (both = NULL)

-- This is useful for analyzing data across multiple dimensions in a single query.

-- GROUPING_ID tells us what type of row it is:
--   - 0 = department + location (normal)
--   - 1 = department subtotal
--   - 2 = location subtotal
--   - 3 = grand total

-- See https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-syntax-qry-select-groupby

SELECT 
  department, 
  location, 
  SUM(salary)           AS total_salary,
  ROUND(AVG(salary), 2) AS avg_salary,
  MAX(salary)           AS max_salary,
  MIN(salary)           AS min_salary,
  GROUPING_ID(department, location) AS grouping_level
FROM employee_analytics
GROUP BY CUBE(department, location)
ORDER BY department, location;


-- COMMAND ----------

-- Creating a View with CUBE Aggregation
-- --------------------------------------
-- This view summarizes total, average, min and max salaries
-- across all possible combinations of department and location.
-- It's built using the CUBE extension and includes GROUPING_ID
-- to identify the aggregation level for each row.

CREATE OR REPLACE VIEW v_cube_salary_summary AS
SELECT 
  department, 
  location, 
  SUM(salary)           AS total_salary,
  ROUND(AVG(salary), 2) AS avg_salary,
  MAX(salary)           AS max_salary,
  MIN(salary)           AS min_salary,
  GROUPING_ID(department, location) AS grouping_level
FROM employee_analytics
GROUP BY CUBE(department, location);


-- COMMAND ----------

-- Querying the CUBE view
-- ------------------------
-- The view `v_cube_salary_summary` contains all combinations of 
-- department and location groupings, including subtotals and the grand total.

SELECT * 
FROM v_cube_salary_summary
ORDER BY department, location;


-- COMMAND ----------

-- Filtering subtotal rows by department (GROUPING_ID = 1)
-- ---------------------------------------------------------
-- This includes one row per department, regardless of location.

SELECT * 
FROM v_cube_salary_summary
WHERE grouping_level = 1
ORDER BY department;


-- COMMAND ----------

-- Filtering subtotal rows by location (GROUPING_ID = 2)
-- -------------------------------------------------------
-- This includes one row per location, across all departments.

SELECT * 
FROM v_cube_salary_summary
WHERE grouping_level = 2
ORDER BY location;


-- COMMAND ----------

-- Filtering the grand total row (GROUPING_ID = 3)
-- ------------------------------------------------
-- This is the total salary stats for the entire dataset.

SELECT * 
FROM v_cube_salary_summary
WHERE grouping_level = 3;


-- COMMAND ----------

-- GROUPING SETS Example: Custom Groupings
-- ----------------------------------------
-- GROUPING SETS gives us full control over which groupings we want.
-- Unlike ROLLUP or CUBE (which follow a pattern), here we define explicitly:
--   - Exactly which grouping combinations we want
--   - In what order

-- In this example, we'll get:
--   - Grouping by department only
--   - Grouping by location only
--   - Grand total (no grouping columns)

-- This allows for highly customized reports.

SELECT 
  department, 
  location, 
  SUM(salary)           AS total_salary,
  ROUND(AVG(salary), 2) AS avg_salary,
  MAX(salary)           AS max_salary,
  MIN(salary)           AS min_salary,
  GROUPING_ID(department, location) AS grouping_level
FROM employee_analytics
GROUP BY GROUPING SETS (
  (department),
  (location),
  ()
)
ORDER BY department, location;


-- COMMAND ----------

-- Creating a View with GROUPING SETS
-- ------------------------------------
-- This view shows salary aggregations grouped by:
--   - department
--   - location
--   - grand total (no grouping columns)
-- GROUPING SETS gives us full manual control over the groupings we want to include.

CREATE OR REPLACE VIEW v_groupingsets_salary_summary AS
SELECT 
  department, 
  location, 
  SUM(salary)           AS total_salary,
  ROUND(AVG(salary), 2) AS avg_salary,
  MAX(salary)           AS max_salary,
  MIN(salary)           AS min_salary,
  GROUPING_ID(department, location) AS grouping_level
FROM employee_analytics
GROUP BY GROUPING SETS (
  (department),
  (location),
  ()
);


-- COMMAND ----------

-- Querying the GROUPING SETS view
-- ---------------------------------
-- This view includes custom groupings:
--   - Aggregation by department
--   - Aggregation by location
--   - Grand total (no grouping columns)

SELECT * 
FROM v_groupingsets_salary_summary
ORDER BY department, location;


-- COMMAND ----------

/*

Summary: ROLLUP vs CUBE vs GROUPING SETS
----------------------------------------

ROLLUP:
  - Produces hierarchical subtotals.
  - Think of it as a top-down summary.
  - Example: ROLLUP(department, location)
      → group by (department, location)
      → subtotal by department
      → grand total

CUBE:
  - Produces all combinations of groupings (cross-tab style).
  - Includes: by department, by location, by both, and total.
  - Example: CUBE(department, location)
      → group by (department, location)
      → group by (department)
      → group by (location)
      → grand total

GROUPING SETS:
  - Fully customizable.
  - You define exactly which groupings you want.
  - Example:
      GROUPING SETS (
        (department),
        (location),
        ()
      )
      → just the department totals, location totals, and overall total
      → no full cross-product unless you explicitly ask for it

GROUPING_ID:
  - A function that helps identify which fields are NULL due to grouping.
  - Bitmask pattern:
      0 = full group
      1 = location subtotal
      2 = department subtotal
      3 = grand total

*/

-- COMMAND ----------

-- REVIEW QUESTIONS --
-- --------------------

-- 1. What is the main difference between ROLLUP and CUBE?

-- 2. Which SQL clause allows you to define exactly which groupings you want?
--     a) GROUP BY
--     b) ROLLUP
--     c) CUBE
--     d) GROUPING SETS

-- 3. What does GROUPING_ID(department, location) = 3 mean?

-- 4. True or False:
--     GROUPING SETS can generate the same results as ROLLUP or CUBE if configured properly.

-- 5. Which of the following statements about views is correct?
--     a) Views store physical data.
--     b) Views can’t be filtered.
--     c) Views behave like virtual tables and can encapsulate complex queries.
--     d) Views require a primary key.

-- 6. You need to calculate salary subtotals by department only. Which of the following should you use?
--     a) ROLLUP(department, location)
--     b) CUBE(department, location)
--     c) GROUPING SETS((department))
--     d) GROUP BY department, location

-- ----------------------------------------------------
-- Answers:
-- 1. ROLLUP produces hierarchical subtotals; CUBE produces all combinations.
-- 2. d) GROUPING SETS
-- 3. Both department and location are NULL → grand total.
-- 4. True
-- 5. c) Views behave like virtual tables and can encapsulate complex queries.
-- 6. c) GROUPING SETS((department))
