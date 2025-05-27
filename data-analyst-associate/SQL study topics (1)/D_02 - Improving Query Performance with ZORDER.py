# Databricks notebook source
# MAGIC %sql
# MAGIC /*  
# MAGIC ==============================================================================
# MAGIC ZORDER AND DATA ORGANIZATION IN DELTA LAKE
# MAGIC ==============================================================================
# MAGIC
# MAGIC What is `ZORDER`?
# MAGIC - `ZORDER` is a data organization technique used in Delta Lake to improve 
# MAGIC   query performance when filtering data on specific columns.
# MAGIC - Instead of physically partitioning the data, `ZORDER` reorders files 
# MAGIC   within existing partitions to group similar values together.
# MAGIC - It's especially useful when a table cannot be efficiently partitioned 
# MAGIC   due to a high number of distinct values in a column.
# MAGIC
# MAGIC ==============================================================================
# MAGIC
# MAGIC Differences between `PARTITION BY` and `ZORDER BY`
# MAGIC - `PARTITION BY` physically separates data into folder structures based on a column, 
# MAGIC   which is efficient if queries frequently filter on that column.
# MAGIC - `ZORDER BY` **does not create new partitions**, it reorganizes data 
# MAGIC   within existing partitions to improve filter performance.
# MAGIC - Use `PARTITION BY` when the filter column has low cardinality 
# MAGIC   (e.g., "year" or "month").
# MAGIC - Use `ZORDER BY` when the filter column has high cardinality 
# MAGIC   (e.g., customer ID or unique identifiers).
# MAGIC
# MAGIC ==============================================================================
# MAGIC
# MAGIC Example usage of `ZORDER`:
# MAGIC
# MAGIC OPTIMIZE table_name ZORDER BY (column1);
# MAGIC This compacts the table and reorganizes the data based on column1.
# MAGIC It helps group similar values together, improving filter and read efficiency.
# MAGIC
# MAGIC ==============================================================================
# MAGIC
# MAGIC When to use ZORDER:
# MAGIC - When queries frequently filter on high-cardinality columns.
# MAGIC - When queries use JOINs on non-partitioned columns.
# MAGIC - When `PARTITION BY` is not feasible due to too many unique values in a column.
# MAGIC
# MAGIC ==============================================================================
# MAGIC
# MAGIC Verifying ZORDER optimization with DESCRIBE HISTORY:
# MAGIC
# MAGIC DESCRIBE HISTORY table_name;
# MAGIC This shows how many files were reordered (ZORDER applied) and helps 
# MAGIC verify improvements in storage layout and performance.
# MAGIC
# MAGIC ==============================================================================
# MAGIC
# MAGIC Precautions when using ZORDER:
# MAGIC - ZORDER does not replace PARTITION BY, it complements it.
# MAGIC - Applying it on too many columns may reduce its effectiveness 
# MAGIC   or negatively impact performance.
# MAGIC - Always run `OPTIMIZE` before applying ZORDER to enable file compaction.
# MAGIC
# MAGIC ==============================================================================
# MAGIC
# MAGIC Conclusion:
# MAGIC ZORDER improves query performance by clustering similar values together 
# MAGIC within Delta table files.
# MAGIC It's particularly helpful when filtering on high-cardinality columns.
# MAGIC Use it strategically to maximize performance gains.
# MAGIC ============================================================================== 
# MAGIC */
# MAGIC

# COMMAND ----------

# =======================================================================================
# CHAPTER D_02 - Improving Query Performance with ZORDER in Delta Lake
# =======================================================================================
#
# OBJECTIVE
# ---------------------------------------------------------------------------------------
# This notebook demonstrates how to improve query performance in Delta Lake
# using the `OPTIMIZE` and `ZORDER` commands.
# It simulates real-world scenarios where query filters and scan performance 
# benefit from strategic file compaction and data clustering.
#
# KEY CONCEPTS COVERED
# ---------------------------------------------------------------------------------------
# What is `ZORDER` and how it differs from `PARTITION BY`
# When and why to apply ZORDER
# How to simulate fragmentation in Delta tables
# Interpreting query execution plans with `EXPLAIN FORMATTED`
# Understanding PhotonScan, DictionaryFilters, and optimizer statistics
# Using `ANALYZE TABLE` to enable filter pushdown and improve scan efficiency
# Adding new columns (like pseudo-random values) and tracking their impact
#
# WORKFLOW SUMMARY
# ---------------------------------------------------------------------------------------
# 1. Set up a user-specific schema and storage environment using widgets
# 2. Create a Delta table (`employees_zorder`) with department and salary data
# 3. Insert 300 records in small batches to simulate file fragmentation
# 4. Run `OPTIMIZE ... ZORDER BY (department, salary)` to compact files and cluster data
# 5. Use `DESCRIBE HISTORY` to verify ZORDER effectiveness
# 6. Execute queries with different filter combinations:
#     - Single column (e.g., department or salary)
#     - Multi-column (e.g., department AND salary)
#     - Non-ZORDERed columns (e.g., id)
# 7. Add a derived column `rs_value` to simulate randomized search behavior
# 8. Use `ANALYZE TABLE` to ensure column-level statistics are available
# 9. Compare physical plans and optimizer behavior before and after statistics are applied
#
# BEST PRACTICES
# ---------------------------------------------------------------------------------------
# - Always apply `ANALYZE TABLE` after modifying schema or inserting large volumes of data
# - Use `ZORDER` for high-cardinality columns frequently used in filters or joins
# - Avoid using non-deterministic functions like `RAND()` in UPDATE/MERGE operations
# - Monitor query plans and file counts to verify actual performance benefits
#
# FINAL NOTES
# ---------------------------------------------------------------------------------------
# - The metrics provided in this notebook (file counts, sizes, statistics) are sample values
#   and may vary based on cluster size, number of partitions, and data layout.
# - This hands-on simulation is designed to prepare you for optimizing real-world Delta tables
#   at scale, especially in analytical workloads where performance is critical.
#
# =======================================================================================


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

# =======================================================================================
# CREATE DELTA TABLE TO APPLY `ZORDER`
# =======================================================================================
# This table simulates employees across different departments and salaries.
# The dataset includes multiple distinct values to demonstrate how `ZORDER` works.
# =======================================================================================

from pyspark.sql.types import IntegerType, StringType, StructType, StructField

# Get user schema from widget
schema = dbutils.widgets.get("user_schema")
table_name = f"{schema}.employees_zorder"

# Define schema
schema_def = StructType([
    StructField("id", IntegerType(), False),
    StructField("department", StringType(), False),
    StructField("salary", IntegerType(), False)
])

# Sample data
data = [
    (1, "Sales", 5000),
    (2, "IT", 7000),
    (3, "HR", 6000),
    (4, "Sales", 5200),
    (5, "IT", 7200),
    (6, "HR", 6100),
    (7, "Sales", 5300),
    (8, "IT", 7300),
    (9, "HR", 6200),
]

# Create DataFrame
df = spark.createDataFrame(data, schema=schema_def)

# Drop table if it exists
spark.sql(f"DROP TABLE IF EXISTS {table_name}")

# Write table as Delta
df.write.format("delta").mode("overwrite").saveAsTable(table_name)

print(f"Delta table {table_name} created successfully for ZORDER testing.")


# COMMAND ----------

# MAGIC %sql
# MAGIC -- =======================================================================================
# MAGIC -- VISUALIZE TABLE CONTENT BEFORE APPLYING ZORDER
# MAGIC -- =======================================================================================
# MAGIC -- This query displays the full contents of the table before applying any ZORDER operation.
# MAGIC -- It's useful for visually comparing row order and structure before and after optimization.
# MAGIC -- =======================================================================================
# MAGIC
# MAGIC SELECT * FROM ${user_schema}.employees_zorder;
# MAGIC
# MAGIC

# COMMAND ----------

# =======================================================================================
# INSERT 300 RECORDS IN MULTIPLE TRANSACTIONS TO SIMULATE FRAGMENTATION
# =======================================================================================
# The `id` column is explicitly typed as IntegerType to avoid Delta type errors.
# Records are inserted in small batches (50 rows at a time) to create small files.
# =======================================================================================

from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# Get user schema from widget
schema = dbutils.widgets.get("user_schema")
table_name = f"{schema}.employees_zorder"

# Define schema
schema_def = StructType([
    StructField("id", IntegerType(), False),
    StructField("department", StringType(), False),
    StructField("salary", IntegerType(), False)
])

# Generate 300 rows with alternating department names and salary ranges
data_bulk = [
    (
        i,
        "Sales" if i % 3 == 1 else "IT" if i % 3 == 2 else "HR",
        5000 + (i % 10) * 100
    )
    for i in range(100, 400)
]

# Create DataFrame
df_bulk = spark.createDataFrame(data_bulk, schema=schema_def)

# Insert in small batches to simulate file fragmentation
for _ in range(0, 300, 50):  # Repeat 6 times
    df_bulk.limit(50).write.format("delta").mode("append").saveAsTable(table_name)

print("300 records inserted in multiple transactions to simulate file fragmentation.")


# COMMAND ----------

# MAGIC %sql
# MAGIC -- =======================================================================================
# MAGIC -- APPLY `OPTIMIZE` AND `ZORDER` ON THE DELTA TABLE
# MAGIC -- =======================================================================================
# MAGIC -- This command applies `ZORDER BY department, salary` to improve
# MAGIC -- query performance when filtering first by `department` and then by `salary`.
# MAGIC --
# MAGIC -- NOTE: The metrics below are based on a previous run and shown for illustration only.
# MAGIC -- Actual values may vary depending on data layout, cluster performance, and file size.
# MAGIC -- 
# MAGIC -- Example output:
# MAGIC -- numFilesRemoved:        7
# MAGIC -- numFilesAdded:          1
# MAGIC -- filesRemoved.totalSize: 9114 bytes
# MAGIC -- filesAdded.totalSize:   1573 bytes
# MAGIC -- numOutputCubes:         1
# MAGIC -- =======================================================================================
# MAGIC
# MAGIC OPTIMIZE ${user_schema}.employees_zorder ZORDER BY (department, salary);
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- =======================================================================================
# MAGIC -- VISUALIZE TABLE CONTENT AFTER APPLYING ZORDER
# MAGIC -- =======================================================================================
# MAGIC -- This query displays the full contents of the table after applying any ZORDER operation.
# MAGIC -- It's useful for visually comparing row order and structure before and after optimization.
# MAGIC -- =======================================================================================
# MAGIC
# MAGIC SELECT * FROM ${user_schema}.employees_zorder;
# MAGIC
# MAGIC -- NOTE:
# MAGIC -- The row order returned by `SELECT * FROM employees_zorder` will look the same
# MAGIC -- before and after ZORDER is applied.
# MAGIC --
# MAGIC -- Why? Because:
# MAGIC -- - ZORDER rearranges data at the **storage/file level**, not at query output level
# MAGIC -- - SQL queries without an `ORDER BY` clause return data in an arbitrary order
# MAGIC -- - Delta Lake doesn't guarantee logical row ordering unless explicitly sorted in the query
# MAGIC --
# MAGIC -- To confirm ZORDER effectiveness, rely on:
# MAGIC -- - `DESCRIBE HISTORY` (to see files added/removed and ZORDER columns used)
# MAGIC -- - `EXPLAIN FORMATTED` (to see filter pushdown and scan efficiency)
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- =======================================================================================
# MAGIC -- VERIFY `ZORDER` OPTIMIZATION DETAILS
# MAGIC -- =======================================================================================
# MAGIC -- `DESCRIBE HISTORY` displays metadata from the OPTIMIZE operation,
# MAGIC -- including how many files were compacted and which columns were used in ZORDER.
# MAGIC --
# MAGIC -- NOTE: The values below are from a previous run and are shown for illustration only.
# MAGIC -- Your actual metrics may differ depending on the specific data and environment.
# MAGIC --
# MAGIC -- Example output:
# MAGIC -- operation:         OPTIMIZE
# MAGIC -- zOrderBy:          ["department", "salary"]
# MAGIC -- numRemovedFiles:   7
# MAGIC -- numAddedFiles:     1
# MAGIC -- numRemovedBytes:   9114
# MAGIC -- numAddedBytes:     1573
# MAGIC -- =======================================================================================
# MAGIC
# MAGIC DESCRIBE HISTORY ${user_schema}.employees_zorder;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- =======================================================================================
# MAGIC -- ANALYZE QUERY FILTERING BY `department` ONLY
# MAGIC -- =======================================================================================
# MAGIC -- We use `EXPLAIN FORMATTED` to verify whether the query benefits from `ZORDER`
# MAGIC -- and how many files are being scanned at runtime.
# MAGIC -- This helps evaluate the effectiveness of the clustering optimization.
# MAGIC -- =======================================================================================
# MAGIC
# MAGIC EXPLAIN FORMATTED 
# MAGIC SELECT * FROM ${user_schema}.employees_zorder WHERE department = 'IT';
# MAGIC
# MAGIC -- =======================================================================================
# MAGIC -- INTERPRETING EXPLAIN FORMATTED RESULTS
# MAGIC -- =======================================================================================
# MAGIC -- The query plan (in my environment) shows:
# MAGIC -- - `PhotonScan` confirms efficient scan execution
# MAGIC -- - `DictionaryFilters` and `RequiredDataFilters` confirm the filter was pushed down
# MAGIC -- - However, missing statistics may limit the full benefits of ZORDER
# MAGIC --
# MAGIC -- To improve query planning and filtering efficiency, run:
# MAGIC -- ANALYZE TABLE ${user_schema}.employees_zorder COMPUTE STATISTICS FOR ALL COLUMNS;
# MAGIC -- =======================================================================================
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Collect column-level statistics to help the optimizer
# MAGIC ANALYZE TABLE ${user_schema}.employees_zorder COMPUTE STATISTICS FOR ALL COLUMNS;
# MAGIC
# MAGIC -- =======================================================================================
# MAGIC -- VERIFYING TABLE STATISTICS AFTER ANALYZE
# MAGIC -- =======================================================================================
# MAGIC -- This output confirms that statistics were successfully computed.
# MAGIC --
# MAGIC -- Key sections to observe:
# MAGIC -- - `Statistics`: shows total size in bytes and number of rows (e.g., "1559 bytes, 309 rows")
# MAGIC --   This is used by the query optimizer to improve scan planning and filtering efficiency.
# MAGIC --
# MAGIC -- - `Delta Statistics Columns`: lists columns with available stats (e.g., id, department, salary)
# MAGIC --   This means column-level statistics are now available for filter and join optimization.
# MAGIC --
# MAGIC -- - `Table Properties`: shows supported Delta features and version compatibility
# MAGIC --   (e.g., deletion vectors, append-only support, reader/writer versions)
# MAGIC --
# MAGIC -- With statistics in place, ZORDER and filter queries can now benefit from
# MAGIC -- more accurate file pruning and improved performance.
# MAGIC -- =======================================================================================
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- =======================================================================================
# MAGIC -- ANALYZE QUERY FILTERING BY `department` AND `salary`
# MAGIC -- =======================================================================================
# MAGIC -- This query is expected to be even more efficient because it follows the
# MAGIC -- same column order defined in `ZORDER BY (department, salary)`.
# MAGIC -- 
# MAGIC -- Use `EXPLAIN FORMATTED` to check how many files are scanned and
# MAGIC -- whether the filter is pushed down in the physical plan.
# MAGIC -- =======================================================================================
# MAGIC
# MAGIC EXPLAIN FORMATTED 
# MAGIC SELECT * 
# MAGIC FROM ${user_schema}.employees_zorder 
# MAGIC WHERE department = 'IT' AND salary > 7000;
# MAGIC
# MAGIC -- =======================================================================================
# MAGIC -- INTERPRETING EXPLAIN FORMATTED RESULTS
# MAGIC -- =======================================================================================
# MAGIC -- The physical plan confirms that the query is fully optimized:
# MAGIC --
# MAGIC -- - `PhotonScan` is used, meaning execution is handled efficiently by Photon engine
# MAGIC -- - `DictionaryFilters` include both: (department = 'IT') AND (salary > 7000)
# MAGIC --   which matches the ZORDER column order: (department, salary)
# MAGIC -- - `RequiredDataFilters` confirms that filter pushdown is applied
# MAGIC -- - Optimizer statistics show: full = employees_zorder → ANALYZE TABLE was successful
# MAGIC --
# MAGIC -- This plan indicates that ZORDER and column statistics are now being leveraged
# MAGIC -- to reduce the number of files scanned and accelerate the query.
# MAGIC -- =======================================================================================
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- =======================================================================================
# MAGIC -- ANALYZE QUERY FILTERING BY `salary` ONLY
# MAGIC -- =======================================================================================
# MAGIC -- Since `ZORDER` was applied on (department, salary), this query only uses
# MAGIC -- the second column in the sort order. As a result, it may require scanning
# MAGIC -- more files compared to queries that also filter by `department`.
# MAGIC -- =======================================================================================
# MAGIC
# MAGIC EXPLAIN FORMATTED 
# MAGIC SELECT * 
# MAGIC FROM ${user_schema}.employees_zorder 
# MAGIC WHERE salary = 7000;
# MAGIC
# MAGIC -- =======================================================================================
# MAGIC -- INTERPRETING EXPLAIN RESULTS FOR SINGLE-COLUMN FILTER (salary)
# MAGIC -- =======================================================================================
# MAGIC -- This query only filters by the `salary` column, which was the second column
# MAGIC -- in the `ZORDER BY (department, salary)` clause. As a result:
# MAGIC --
# MAGIC -- - `PhotonScan` confirms efficient execution using the Photon engine
# MAGIC -- - `DictionaryFilters` and `RequiredDataFilters` show that filter pushdown is applied
# MAGIC --   for `(salary = 7000)`
# MAGIC -- - Since the filter is not on the leading column (`department`), the optimizer may need
# MAGIC --   to scan more files than if both columns were included
# MAGIC -- - Statistics state shows `full = employees_zorder`, which confirms ANALYZE was applied
# MAGIC --
# MAGIC -- Overall: query runs efficiently, but benefits from ZORDER may be limited when only
# MAGIC -- filtering by the second column.
# MAGIC -- =======================================================================================
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- =======================================================================================
# MAGIC -- EXPLAIN QUERY THAT SCANS FULL RANGE OF `id` VALUES
# MAGIC -- =======================================================================================
# MAGIC -- This query returns the minimum and maximum values of the `id` column.
# MAGIC -- We use `EXPLAIN FORMATTED` to observe how the query is executed and whether
# MAGIC -- the optimizer takes advantage of statistics or pruning.
# MAGIC -- =======================================================================================
# MAGIC
# MAGIC EXPLAIN FORMATTED
# MAGIC SELECT MIN(id) AS min_id, MAX(id) AS max_id
# MAGIC FROM ${user_schema}.employees_zorder;
# MAGIC
# MAGIC -- =======================================================================================
# MAGIC -- INTERPRETING EXPLAIN RESULTS FOR AGGREGATION QUERY (MIN/MAX)
# MAGIC -- =======================================================================================
# MAGIC -- - The plan shows `LocalTableScan`, which indicates that the result was computed
# MAGIC --   from cached metadata or small internal aggregates, not a full file scan.
# MAGIC --
# MAGIC -- - This is common for MIN/MAX queries on optimized and well-analyzed Delta tables.
# MAGIC -- - It may also happen because of previously computed column-level statistics from
# MAGIC --   `ANALYZE TABLE`, which allow the engine to avoid reading physical files.
# MAGIC --
# MAGIC -- - Photon shows: "Photon does not fully support the query" due to `LocalTableScan`,
# MAGIC --   but this does not indicate a performance issue — it's simply a fallback to standard Spark execution.
# MAGIC -- =======================================================================================
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- =======================================================================================
# MAGIC -- EXPLAIN QUERY THAT AGGREGATES SALARY BY DEPARTMENT
# MAGIC -- =======================================================================================
# MAGIC -- This query groups employees by `department` and calculates the total salary per group.
# MAGIC -- We use `EXPLAIN FORMATTED` to observe whether ZORDER or statistics help optimize the scan.
# MAGIC -- =======================================================================================
# MAGIC
# MAGIC EXPLAIN FORMATTED
# MAGIC SELECT department, SUM(salary) AS total_salary
# MAGIC FROM ${user_schema}.employees_zorder
# MAGIC GROUP BY department;
# MAGIC
# MAGIC -- =======================================================================================
# MAGIC -- INTERPRETING AGGREGATION PLAN (SUM BY DEPARTMENT)
# MAGIC -- =======================================================================================
# MAGIC -- - `PhotonScan` confirms efficient file reading using the Photon engine.
# MAGIC -- - The aggregation is performed via `PhotonGroupingAgg`, which is optimized
# MAGIC --   for grouped computations.
# MAGIC -- - `PhotonShuffleExchangeSink` shows that Spark repartitions data by `department`
# MAGIC --   to perform grouped aggregation in parallel.
# MAGIC -- - The plan includes a two-phase aggregation:
# MAGIC --     - Partial aggregation (map-side)
# MAGIC --     - Final aggregation (reduce-side)
# MAGIC -- - `AdaptiveSparkPlan` indicates Spark is dynamically adjusting execution
# MAGIC --   based on runtime statistics and file sizes.
# MAGIC -- - Optimizer Statistics: `full = employees_zorder`, which confirms that
# MAGIC --   ANALYZE TABLE was effective and statistics are available for planning.
# MAGIC --
# MAGIC -- Overall: This is an efficient execution plan for grouped aggregation
# MAGIC -- in an optimized Delta table.
# MAGIC -- =======================================================================================
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- =======================================================================================
# MAGIC -- VERIFY TABLE STATISTICS AND METADATA
# MAGIC -- =======================================================================================
# MAGIC -- This command returns detailed information about the Delta table, including:
# MAGIC -- - Column-level statistics
# MAGIC -- - Number of files and total size
# MAGIC -- - Table location, schema, format, and optimization features
# MAGIC -- - Whether ZORDER or OPTIMIZE have been applied
# MAGIC -- =======================================================================================
# MAGIC
# MAGIC DESCRIBE DETAIL ${user_schema}.employees_zorder;
# MAGIC
# MAGIC -- =======================================================================================
# MAGIC -- INTERPRETING DESCRIBE DETAIL RESULTS
# MAGIC -- =======================================================================================
# MAGIC -- This output provides low-level metadata for the Delta table:
# MAGIC --
# MAGIC -- - `format`: The table uses Delta format.
# MAGIC -- - `location`: Shows where the table is stored in S3.
# MAGIC -- - `createdAt`, `lastModified`: Timestamps for when the table was created and last written.
# MAGIC -- - `partitionColumns`: Empty, meaning the table is not physically partitioned.
# MAGIC -- - `clusteringColumns`: Empty, ZORDER is used instead of partitioning.
# MAGIC -- - `numFiles`: Only 1 file remains after optimization (was >5 before).
# MAGIC -- - `sizeInBytes`: Total storage size (e.g., 1559 bytes).
# MAGIC -- - `properties`: Includes Delta features like deletion vectors and invariants.
# MAGIC -- - `tableFeatures`: Shows enabled features like `appendOnly`, `deletionVectors`.
# MAGIC -- - `statistics`: No deleted rows; no deletion vectors applied.
# MAGIC --
# MAGIC -- This confirms the table has been compacted, analyzed, and optimized
# MAGIC -- with ZORDER — and is ready for efficient querying.
# MAGIC -- =======================================================================================
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- =======================================================================================
# MAGIC -- ANALYZE QUERY FILTERING BY `id` ONLY (Not Optimized by ZORDER)
# MAGIC -- =======================================================================================
# MAGIC -- In this case, the query filters by `id`, which was NOT included in the ZORDER clause.
# MAGIC -- As a result, the query may scan more data files, since no data clustering exists
# MAGIC -- on this column.
# MAGIC -- =======================================================================================
# MAGIC
# MAGIC EXPLAIN FORMATTED 
# MAGIC SELECT * 
# MAGIC FROM ${user_schema}.employees_zorder 
# MAGIC WHERE id = 50;
# MAGIC
# MAGIC -- =======================================================================================
# MAGIC -- INTERPRETING EXPLAIN RESULTS: FILTER ON NON-ZORDERED COLUMN
# MAGIC -- =======================================================================================
# MAGIC -- Although `id` was NOT part of the ZORDER clause:
# MAGIC --
# MAGIC -- - `DictionaryFilters` and `RequiredDataFilters` confirm that the filter `(id = 50)`
# MAGIC --   was pushed down to the scan level.
# MAGIC -- - Since the table was ANALYZED, column-level stats help avoid scanning unnecessary files.
# MAGIC -- - In this case, only one file exists (after OPTIMIZE), so no pruning was needed.
# MAGIC -- - Photon executed the query efficiently, as shown by `PhotonScan`.
# MAGIC --
# MAGIC -- Conclusion:
# MAGIC -- Even when a column is not part of ZORDER, small optimized tables with up-to-date
# MAGIC -- statistics can still benefit from filter pushdown and efficient execution.
# MAGIC -- =======================================================================================
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- =======================================================================================
# MAGIC -- ADDING A DERIVED COLUMN FOR RANDOMIZED SEARCH SIMULATION (`rs_value`)
# MAGIC -- =======================================================================================
# MAGIC -- GOAL:
# MAGIC -- We simulate a scenario where queries need to filter on a numeric column that behaves
# MAGIC -- similarly to a pseudo-random value — without using non-deterministic functions like RAND().
# MAGIC --
# MAGIC -- CONTEXT:
# MAGIC -- Delta Lake does not allow non-deterministic functions like `RAND()` inside UPDATE or MERGE.
# MAGIC -- To simulate randomness in a controlled, stable way, we use a deterministic expression:
# MAGIC --
# MAGIC --     rs_value = (salary % 3) + (id / 1000.0)
# MAGIC --
# MAGIC -- This generates:
# MAGIC -- - A base value of 0, 1, or 2 depending on the modulo of salary
# MAGIC -- - A decimal component from the `id` that ensures "uniqueness" per row
# MAGIC --
# MAGIC -- BENEFITS:
# MAGIC -- - `rs_value` will range from approximately 0.000 to just under 3.000
# MAGIC -- - Values are stable across reruns, making them suitable for filtering
# MAGIC -- - No risk of errors in future MERGE or UPDATE operations
# MAGIC --
# MAGIC -- VERIFICATION:
# MAGIC -- A final SELECT is included to preview the new values.
# MAGIC -- =======================================================================================
# MAGIC
# MAGIC -- Add new column
# MAGIC ALTER TABLE ${user_schema}.employees_zorder ADD COLUMNS (rs_value DOUBLE);
# MAGIC
# MAGIC -- Update the column with deterministic pseudo-random values
# MAGIC UPDATE ${user_schema}.employees_zorder 
# MAGIC SET rs_value = salary % 3 + id / 1000.0;
# MAGIC
# MAGIC -- View the results
# MAGIC SELECT * FROM ${user_schema}.employees_zorder limit 10;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- View the results
# MAGIC SELECT * FROM ${user_schema}.employees_zorder limit 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- =======================================================================================
# MAGIC -- ANALYZE QUERY FILTERING BY `rs_value`
# MAGIC -- =======================================================================================
# MAGIC -- This query tests whether the optimizer can effectively filter on the newly added
# MAGIC -- `rs_value` column. We use `EXPLAIN FORMATTED` to check:
# MAGIC -- - If the filter is pushed down using `DictionaryFilters`
# MAGIC -- - Or if Spark needs to scan the full table due to lack of clustering/statistics
# MAGIC -- =======================================================================================
# MAGIC
# MAGIC EXPLAIN FORMATTED 
# MAGIC SELECT * 
# MAGIC FROM ${user_schema}.employees_zorder 
# MAGIC WHERE rs_value = 2.109; -- <-- Use a value from previous results.
# MAGIC
# MAGIC -- =======================================================================================
# MAGIC -- INTERPRETING EXPLAIN RESULTS FOR `rs_value` FILTER
# MAGIC -- =======================================================================================
# MAGIC -- - The plan shows `PhotonScan` is used → the query is executed efficiently with Photon.
# MAGIC -- - `DictionaryFilters`: Spark was able to push down the filter `(rs_value = 2.109)`
# MAGIC --   → This improves scan efficiency, even though `rs_value` was added recently.
# MAGIC -- - `RequiredDataFilters`: Confirms the predicate is applied at the scan level.
# MAGIC --
# MAGIC -- HOWEVER:
# MAGIC -- - Optimizer Statistics: `partial = employees_zorder`, `full =` empty
# MAGIC --   → This means column-level stats are missing for some or all columns.
# MAGIC --   → Spark recommends running:
# MAGIC --
# MAGIC --     ANALYZE TABLE ${user_schema}.employees_zorder COMPUTE STATISTICS FOR ALL COLUMNS;
# MAGIC --
# MAGIC -- Running that will ensure all columns — including `rs_value` — are registered
# MAGIC -- with up-to-date statistics, which helps improve filtering and file pruning.
# MAGIC -- =======================================================================================
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- =======================================================================================
# MAGIC -- COMPUTE STATISTICS FOR ALL COLUMNS (Including `rs_value`)
# MAGIC -- =======================================================================================
# MAGIC -- This command collects full column-level statistics for the Delta table.
# MAGIC -- It enables Spark to make smarter decisions about file pruning, filtering,
# MAGIC -- and join optimizations — especially useful for columns like `rs_value`
# MAGIC -- that were added after the original table creation.
# MAGIC -- =======================================================================================
# MAGIC
# MAGIC ANALYZE TABLE ${user_schema}.employees_zorder COMPUTE STATISTICS FOR ALL COLUMNS;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- =======================================================================================
# MAGIC -- RE-EXECUTE EXPLAIN TO CONFIRM STATISTICS ARE COMPLETE
# MAGIC -- =======================================================================================
# MAGIC -- After running ANALYZE TABLE, this should now show:
# MAGIC -- - Optimizer Statistics: full = employees_zorder
# MAGIC -- - Filter pushdown still applied (DictionaryFilters on `rs_value`)
# MAGIC -- =======================================================================================
# MAGIC
# MAGIC EXPLAIN FORMATTED 
# MAGIC SELECT * 
# MAGIC FROM ${user_schema}.employees_zorder 
# MAGIC WHERE rs_value = 2.109;
# MAGIC
# MAGIC -- =======================================================================================
# MAGIC -- FINAL INTERPRETATION: STATISTICS AND FILTER PUSHDOWN CONFIRMED
# MAGIC -- =======================================================================================
# MAGIC -- - `full = employees_zorder`: Confirms that column-level statistics are now complete.
# MAGIC -- - `DictionaryFilters` and `RequiredDataFilters`: The filter on `rs_value = 2.109`
# MAGIC --   is successfully pushed down to the scan level.
# MAGIC -- - `PhotonScan`: Shows the query is fully handled by Photon for optimal performance.
# MAGIC --
# MAGIC -- IMPACT:
# MAGIC -- Column statistics help the optimizer:
# MAGIC -- - Prune unnecessary data files (especially in larger tables)
# MAGIC -- - Select better join strategies
# MAGIC -- - Plan queries with tighter resource use
# MAGIC --
# MAGIC -- Best practice: Always run `ANALYZE TABLE ... COMPUTE STATISTICS FOR ALL COLUMNS`
# MAGIC -- after altering a Delta table or adding new columns used in WHERE or JOIN clauses.
# MAGIC -- =======================================================================================
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- REVIEW QUESTIONS --
# MAGIC -- The following questions are recommended to reinforce the concepts from this notebook.
# MAGIC
# MAGIC -- 1. What is the purpose of the ZORDER command in Delta Lake?
# MAGIC -- 2. How does ZORDER differ from PARTITION BY?
# MAGIC -- 3. What kind of columns benefit most from ZORDER?
# MAGIC -- 4. What is the purpose of the OPTIMIZE command?
# MAGIC -- 5. How can you verify whether ZORDER has been applied to a table?
# MAGIC -- 6. What does ANALYZE TABLE do, and why is it important after inserting data?
# MAGIC -- 7. What is the impact of column-level statistics on query performance?
# MAGIC -- 8. How can EXPLAIN FORMATTED help evaluate query plans in Delta Lake?
# MAGIC -- 9. Why might a query filtering only by the second ZORDER column be less efficient?
# MAGIC -- 10. What are DictionaryFilters and RequiredDataFilters?
# MAGIC
# MAGIC -- -----------------------------------------------------------------------
# MAGIC
# MAGIC -- ANSWERS --
# MAGIC
# MAGIC -- 1. ZORDER physically reorders data within files to cluster similar values together and improve filter performance.
# MAGIC -- 2. PARTITION BY creates new folders per value; ZORDER rearranges data inside files without new partitions.
# MAGIC -- 3. High-cardinality columns that are frequently used in WHERE filters or JOINs (e.g., customer_id).
# MAGIC -- 4. OPTIMIZE compacts small files into larger ones to improve performance and is required before ZORDER.
# MAGIC -- 5. Use DESCRIBE HISTORY to check recent OPTIMIZE operations and see which columns were used in ZORDER.
# MAGIC -- 6. It computes statistics (e.g., min, max, count) on columns to help Spark's query planner and enable file pruning.
# MAGIC -- 7. Statistics allow the optimizer to skip scanning irrelevant files and choose more efficient execution strategies.
# MAGIC -- 8. It reveals how Spark plans to execute a query and whether filters are pushed down to the scan level.
# MAGIC -- 9. Because ZORDER is most effective when the filter matches the **leading** columns in the ZORDER clause.
# MAGIC -- 10. DictionaryFilters and RequiredDataFilters are internal mechanisms used by Spark to apply filter pushdown during scans.
# MAGIC
