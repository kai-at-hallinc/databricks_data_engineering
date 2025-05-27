-- Databricks notebook source
/*
=======================================================================================
Delta Lake Workshop - Master Index and Execution Guide
=======================================================================================

This notebook provides an overview of the full Delta Lake workshop and serves as a 
guide for understanding how the notebooks are structured, how to run them properly,
and how to use them for self-paced practice or as part of a formal training.

---------------------------------------------------------------------------------------
Structure of the Workshop
---------------------------------------------------------------------------------------

The workshop is organized into thematic modules, each focusing on key Delta Lake topics:

- A_01 to A_03: SQL Basics, Delta Tables, and Merge operations
- B_01 to B_02: File formats (JSON, Parquet, Delta) and incremental pipelines
- D_01 to D_04: Constraints, Time Travel, Table History, Data Recovery
- E_01 to E_02: Data Validation and Real-Time Error Handling
- F_01 to F_04: Special Joins, Array creation and updates, Explode techniques
- Z_99: Cleanup of schema and storage (recommended at the end)

---------------------------------------------------------------------------------------
Notebook Index
---------------------------------------------------------------------------------------

A_01 - SQL and Delta Lake (basic) ................... Create and query Delta tables using SQL  
A_02 - PySpark & merge with DataFrames .............. Perform upserts using PySpark and DeltaTable  
A_03 - Pipeline: CSV → MERGE → Delta ............... Build an ingestion pipeline from CSV to Delta with MERGE  

B_01 - JSON, Parquet, Delta, Optimization ........... Compare file formats and optimize Delta tables  
B_02 - Incremental Merge (simulated) ................ Simulate CDC-like merges without a real database  

D_01 - Data Audit and Quality in Delta Lake ......... Use CHECK constraints and track constraint violations  
D_02 - Improving Query Performance with ZORDER ...... Use OPTIMIZE and ZORDER to improve query speed  
D_03 - Delta Table History and Time Travel .......... Explore version history and query old snapshots  
D_04 - Data Recovery with Restore ................... Revert Delta tables to previous states using RESTORE  

E_01 - Monitoring and Data Correction ............... Validate and correct incoming records post-ingestion  
E_02 - Real-Time Error Handling ..................... Handle invalid data proactively before it hits Delta  

F_01 - Special Joins ................................ Use SEMI, ANTI, and FULL joins for data validation  
F_02 - From Table to Array .......................... Convert grouped rows into arrays (collect_list)  
F_03 - From Array to Table .......................... Flatten arrays using explode and posexplode  
F_04 - Querying and Updating Arrays ................. Access, modify, filter, and update array elements  

G_01 - Grouping Extensions: ROLLUP, CUBE, SETS ...... Generate subtotals and multidimensional aggregations  
G_02 - Window Functions and Row Comparisons ......... Rank rows, access previous/next values, track gaps  
G_03 - Common Table Expressions (CTEs) .............. Structure complex queries with named subqueries  

Z_99 - Cleanup ...................................... Drop user schema and delete files from S3 path

---------------------------------------------------------------------------------------
Common Setup Section
---------------------------------------------------------------------------------------

Every notebook starts with a shared setup block written in Python. This section:

1. Retrieves the current user (`current_user()`). (In this file I'm going to use Peter Spider as user name)
2. Normalizes the username to generate:
   - A unique schema (e.g., `sql_review_peter_spider`)
   - A dedicated S3 path (e.g., `external_data/peter_spider/sql_review`)
3. Creates both schema and folder if they don’t exist.
4. Registers widgets like `user_schema` and `user_path` to be used later in SQL.

This setup ensures that every user runs the notebooks in an isolated and safe environment.

---------------------------------------------------------------------------------------
Final Cleanup Notebook: Z_99
---------------------------------------------------------------------------------------

To clean up all resources created during the workshop, you **must run Z_99**.
This notebook:

- Drops the user-specific schema and tables.
- Deletes files from the user's S3 folder.
- Ensures the workspace is clean for future runs or new users.

It's strongly recommended to run Z_99 at the end of your session or before sharing the workspace.

---------------------------------------------------------------------------------------
Review Questions
---------------------------------------------------------------------------------------

At the end of each notebook, you'll find a section titled:

    -- REVIEW QUESTIONS --

This section contains handpicked certification-style questions (with answers) based on the content of the notebook. 
These are provided to reinforce learning and support knowledge validation. They are **not official certification material** 
but are aligned with typical Databricks topics.

---------------------------------------------------------------------------------------
Out of Scope - Topics Not Covered in This Workshop
---------------------------------------------------------------------------------------

The following topics are important in the Databricks ecosystem or in SQL analytics 
but are not covered in this workshop. They can be explored independently or as part 
of future modules or live sessions:

1. Dashboards and BI Integration
   - Building dashboards in Databricks
   - Connecting with Power BI, Tableau, Looker, etc.

2. Access Control & Permissions
   - GRANT / REVOKE, data masking, user roles, ACLs

3. Query Editor Features
   - Saved queries, query history, SQL editor UI capabilities

4. SQL Warehouses and Cluster Management
   - SQL Warehouse provisioning, cluster scaling, connection setup

5. Delta Live Tables (DLT)
   - `LIVE TABLE`, `expect`, `apply_changes`, `declare`, DLT pipelines

6. Structured Streaming
   - `readStream`, `writeStream`, streaming ingestion, micro-batches

7. Unity Catalog
   - 3-level namespaces: catalog.schema.table
   - Centralized governance and cross-workspace data access

8. Workflow Orchestration
    - DAGs, dependency management, scheduling with Airflow or similar tools

9. External Source Ingestion
    - Loading from relational databases (JDBC), REST APIs, or cloud services

10. Bronze / Silver / Gold Data Architecture
    - Explicit layering and separation of raw, refined, and curated datasets

11. Cost Optimization Strategies
    - Broadcast joins, partition tuning, caching, and job performance tuning

-- These topics are not covered *yet* in this workshop... 
-- but hey, they could totally appear in future notebooks.
-- Maybe you're the brave soul who adds them?
-- We're still waiting for that legendary volunteer to drop the INSERT INTO knowledge...

These topics were excluded to keep the workshop focused on Delta Lake fundamentals,
data engineering patterns, and query performance. They are excellent candidates 
for advanced learning, real-world integrations, or certification preparation.


---------------------------------------------------------------------------------------
Disclaimer
---------------------------------------------------------------------------------------

This workshop is **not intended to replace the official Databricks documentation** or formal certification guides.
Instead, it complements them with real, hands-on exercises and conceptual reinforcement for learners who want to go deeper.

For official information, always refer to:
https://docs.databricks.com/
https://www.databricks.com/learn/certification

=======================================================================================
End of Master Guide
=======================================================================================
*/
