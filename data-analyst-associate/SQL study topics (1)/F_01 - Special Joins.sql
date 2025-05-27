-- Databricks notebook source
/*
=======================================================================================
F_01 - SPECIAL JOINS IN DELTA LAKE / SPARK SQL
=======================================================================================

OBJECTIVE
---------------------------------------------------------------------------------------
This notebook explores several powerful and sometimes lesser-known types of SQL joins 
available in Spark SQL and Delta Lake. These join types go beyond the typical INNER 
and OUTER joins and are essential for real-world data modeling, filtering, and analysis.

These examples are designed for clarity and include comments to help learners understand
when and why to use each type of join. Practical scenarios such as finding unmatched records,
avoiding duplicates in self joins, or generating combinations are all covered here.

JOIN TYPES COVERED
---------------------------------------------------------------------------------------

1. JOIN USING (SSN)
   - A simpler syntax for joining tables on columns with the same name.
   - Cleaner and easier to read than ON a.col = b.col when applicable.

2. SEMI JOIN
   - Returns records from the left table that have at least one match in the right table.
   - Used for existence filtering (e.g., "show employees who have a project").

3. ANTI JOIN
   - The opposite of a SEMI JOIN.
   - Returns rows from the left table that do NOT have a match on the right.
   - Useful for detecting missing or orphan records.

4. CROSS JOIN
   - Produces the Cartesian product between two tables.
   - Every row from table A is joined with every row from table B.
   - Use with caution — the output grows fast!

5. SELF JOIN
   - A table joined with itself.
   - In this notebook, we use it to find employees who work in the same department.
   - Includes a light-hearted explanation of why we use SSN > SSN to avoid duplicate gossip. 😄

6. LEFT OUTER JOIN
   - Returns all records from the left table, and matching ones from the right.
   - Records with no match show NULLs on the right side.

7. RIGHT OUTER JOIN
   - Returns all records from the right table, and matching ones from the left.

8. JOIN with compound conditions
   - Demonstrates how to apply additional filters in the JOIN clause itself.
   - Example: joining only when salary is above a given threshold.

9. FULL OUTER JOIN
   - Includes all records from both sides, whether they match or not.
   - Great for auditing and completeness checks.

TABLES USED
---------------------------------------------------------------------------------------
- `${user_schema}.employee`  
  Contains basic employee information: SSN, first_name, salary, department

- `${user_schema}.employee_project`  
  A secondary table containing project assignments (some valid, some unmatched)

NOTES
---------------------------------------------------------------------------------------
- All joins are written in SQL and dynamically scoped to the user's schema via widgets.
- This notebook is fully isolated and can be safely re-run without breaking state.
- Designed to be both instructional and practical.

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

-- MAGIC %python
-- MAGIC # ---------------------------------------------------------------------------------------
-- MAGIC # Create the main employee table using the user-specific schema
-- MAGIC # ---------------------------------------------------------------------------------------
-- MAGIC
-- MAGIC from datetime import date
-- MAGIC
-- MAGIC # Get user schema from widget
-- MAGIC schema = dbutils.widgets.get("user_schema")
-- MAGIC table_name = f"{schema}.employee"
-- MAGIC
-- MAGIC # Define data
-- MAGIC data = [
-- MAGIC     ("AAAAA", "Amy",     2500, "Sales"),
-- MAGIC     ("BBBBB", "Bruce",   5000, "IT"),
-- MAGIC     ("CCCCC", "Carla",  10000, "HR"),
-- MAGIC     ("DDDDD", "Danielle",   2000, "Finance"),
-- MAGIC     ("EEEEE", "Elena", 8000, "Marketing"),
-- MAGIC     ("FFFFF", "Flavio", 5000, "Marketing")
-- MAGIC ]
-- MAGIC
-- MAGIC columns = ["SSN", "first_name", "salary", "department"]
-- MAGIC
-- MAGIC df_employee = spark.createDataFrame(data, columns)
-- MAGIC
-- MAGIC # Create table (overwrite if it already exists)
-- MAGIC df_employee.write.format("delta").mode("overwrite").saveAsTable(table_name)
-- MAGIC
-- MAGIC print(f"Table created: {table_name}")
-- MAGIC

-- COMMAND ----------

-- =======================================================================================
-- Create the child table: employee_project
-- =======================================================================================
-- This table contains a list of employees assigned to specific projects.
-- Some SSNs match the employee table, others do not (e.g., 'ZZZZZ').
-- This allows us to test different types of joins.
-- =======================================================================================

CREATE OR REPLACE TABLE ${user_schema}.employee_project (
  SSN STRING,
  project_name STRING
) USING DELTA;

INSERT INTO ${user_schema}.employee_project (SSN, project_name) VALUES
  ('AAAAA', 'Website Redesign'),
  ('BBBBB', 'Cloud Migration'),
  ('ZZZZZ', 'Security Audit'); -- This SSN does not exist in the employee table


-- COMMAND ----------

-- =======================================================================================
-- SEMI JOIN: Employees who have at least one matching project
-- =======================================================================================
-- This returns rows from the employee table where a match exists in employee_project.
-- Only columns from the left table (employee) are returned.
-- Think of it as: "Filter employees who are assigned to at least one project."
-- =======================================================================================

SELECT *
FROM ${user_schema}.employee
SEMI JOIN ${user_schema}.employee_project
ON employee.SSN = employee_project.SSN;


-- COMMAND ----------

-- =======================================================================================
-- ANTI JOIN: Employees with no assigned projects
-- =======================================================================================
-- This returns rows from the employee table that have no matching SSN in employee_project.
-- Only columns from the left table (employee) are returned.
-- Think of it as: "Show employees who are not assigned to any project."
-- =======================================================================================

SELECT *
FROM ${user_schema}.employee
ANTI JOIN ${user_schema}.employee_project
ON employee.SSN = employee_project.SSN;


-- COMMAND ----------

-- =======================================================================================
-- CROSS JOIN: All possible combinations between employees and projects
-- =======================================================================================
-- This join returns the Cartesian product between both tables.
-- Every row from employee is combined with every row from employee_project.
-- Use with caution — the number of rows returned is: (#employees) × (#projects)
-- =======================================================================================

SELECT *
FROM ${user_schema}.employee as E
CROSS JOIN ${user_schema}.employee_project as P
-- Try running the query as-is to see the full Cartesian product.
-- Then uncomment the WHERE clause below to apply a condition
-- and reduce the number of combinations to only matched pairs.

-- WHERE E.SSN = P.SSN;


-- COMMAND ----------

-- =======================================================================================
-- SELF JOIN: Employees working in the same department
-- =======================================================================================
-- This joins the employee table with itself to find pairs of employees
-- who work in the same department.
--
-- To avoid showing Amy matched with... Amy, we filter out self-matches.
-- We can use either <> or >:
--   - <> gives both (Amy, Bruce) and (Bruce, Amy)
--   - > gives just one (e.g., Amy, Bruce) and avoids duplicate pairs
-- In short: > saves us the embarrassment of printing the same gossip twice :-)
-- =======================================================================================


SELECT 
  e1.first_name AS employee_1,
  e2.first_name AS employee_2,
  e1.department
FROM ${user_schema}.employee e1
JOIN ${user_schema}.employee e2
  ON e1.department = e2.department
WHERE e1.SSN > e2.SSN;


-- COMMAND ----------

-- =======================================================================================
-- LEFT OUTER JOIN: All employees, with or without a project
-- =======================================================================================
-- This returns all employees and their projects (if any).
-- If an employee has no project, the project columns will be NULL.
-- =======================================================================================

SELECT 
  e.first_name,
  e.department,
  p.project_name
FROM ${user_schema}.employee e
LEFT OUTER JOIN ${user_schema}.employee_project p
  ON e.SSN = p.SSN;


-- COMMAND ----------

-- =======================================================================================
-- RIGHT OUTER JOIN: All projects, including those not assigned to any employee
-- =======================================================================================
-- This returns all projects and the employee assigned (if any).
-- Projects with unknown or unmatched SSNs will still appear.
-- =======================================================================================

SELECT 
  e.first_name,
  p.project_name
FROM ${user_schema}.employee e
RIGHT OUTER JOIN ${user_schema}.employee_project p
  ON e.SSN = p.SSN;


-- COMMAND ----------

-- =======================================================================================
-- JOIN with compound condition: Employee assigned to project AND salary > 4000
-- =======================================================================================
-- This joins both tables but adds an extra filter condition (business rule):
-- Only include employees earning more than 4000.
-- =======================================================================================

SELECT 
  e.first_name,
  e.salary,
  p.project_name
FROM ${user_schema}.employee e
JOIN ${user_schema}.employee_project p
  ON e.SSN = p.SSN AND e.salary > 4000;


-- COMMAND ----------

-- =======================================================================================
-- FULL OUTER JOIN: All employees and all projects, matched or not
-- =======================================================================================
-- This includes:
-- - Employees with and without projects
-- - Projects with and without employees
-- It's useful for completeness checks or audits.
-- =======================================================================================

SELECT 
  e.first_name AS employee_name,
  p.project_name
FROM ${user_schema}.employee e
FULL OUTER JOIN ${user_schema}.employee_project p
  ON e.SSN = p.SSN;


-- COMMAND ----------

-- REVIEW QUESTIONS --
-- The following questions are recommended to reinforce the concepts from this notebook.

-- 1. What is the difference between INNER JOIN and LEFT JOIN?
-- 2. When should you use a RIGHT JOIN instead of a LEFT JOIN?
-- 3. What is the purpose of a FULL OUTER JOIN?
-- 4. What does a LEFT ANTI JOIN return?
-- 5. What does a LEFT SEMI JOIN return?
-- 6. How can joins be used to detect data quality issues?
-- 7. Which join type helps identify "orphan" records?
-- 8. What are typical use cases for using SEMI or ANTI joins in data pipelines?
-- 9. How can NULLs affect JOIN results?
-- 10. Why is join condition design important when validating business logic?

-- -----------------------------------------------------------------------

-- ANSWERS --

-- 1. INNER JOIN returns only matching rows; LEFT JOIN returns all rows from the left table and matches from the right (if any).
-- 2. When the focus is on keeping all records from the right table, regardless of whether they match on the left.
-- 3. To retain all rows from both tables, matching where possible and filling with NULLs where not.
-- 4. LEFT ANTI JOIN returns rows from the left table that have **no match** in the right table.
-- 5. LEFT SEMI JOIN returns rows from the left table **that do match** in the right table, without bringing in columns from the right.
-- 6. By revealing missing keys, extra rows, mismatched data, or unexpected NULLs after a JOIN.
-- 7. LEFT ANTI JOIN helps find records that don’t have corresponding entries in a reference table.
-- 8. To filter records before insert, validate referential integrity, or remove bad data.
-- 9. NULLs in join keys prevent matches, potentially causing missing or extra rows depending on the join type.
-- 10. Because incorrect join logic can lead to data duplication, loss, or wrong analytics conclusions.
