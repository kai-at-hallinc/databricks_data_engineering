-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC
-- MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Upgrading tables to Unity Catalog
-- MAGIC
-- MAGIC In this lab you will learn how to:
-- MAGIC * Upgrade a table from the local Hive metastore to Unity Catalog using four different approaches
-- MAGIC * Understand the cases where each approach is appropriate

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Prerequisites
-- MAGIC If you would like to follow along with this notebook, you will need:
-- MAGIC * **USE CATALOG** and **CREATE SCHEMA** privileges on a catalog

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Setup
-- MAGIC
-- MAGIC This notebook assumes you will be working in the *main* catalog of your Unity Catalog metastore, but you need the **`USE CATALOG`** and **`CREATE SCHEMA`** privileges on the catalog to perform this lab. If you have these privileges on a different catalog, then update the value for *catalog* in the next cell before proceeding.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC catalog = "main"

-- COMMAND ----------

-- MAGIC %md
-- MAGIC With the catalog configured, run the following cell to perform some setup. In order to avoid conflicts in a shared training environment, this will create a uniquely named schema exclusively for your use. This will also create an example source table called *movies* within the legacy Hive metastore. 

-- COMMAND ----------

-- MAGIC %run ./Includes/Upgrading-tables-setup

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Exploring the source table
-- MAGIC
-- MAGIC As part of the setup, we now have a table called *movies*, residing in a user-specific schema of the Hive metastore. To make things easier, the schema name is stored in a Hive variable named **`da.schema`**. Let's preview the data stored in this table using that variable. Notice how the three-level namespaces makes referencing data objects in the Hive metastore seamless.

-- COMMAND ----------

SELECT * FROM hive_metastore.${da.schema}.movies LIMIT 10

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Overview of upgrade methods
-- MAGIC
-- MAGIC There are a few different ways to upgrade a table, but the method you choose will be driven primarily by how you want to treat the table data. If you wish to leave the table data in place, then the resulting upgraded table will be an external table. If you wish to move the table data into your Unity Catalog metastore, then the resulting table will be a managed table. It's always preferable to used managed tables whenever possible.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Moving table data into the Unity Catalog metastore
-- MAGIC
-- MAGIC In this approach, table data will be copied from wherever it resides into the managed data storage area for the destination schema, catalog or metastore. The result will be a managed Delta table in your Unity Catalog metastore. 
-- MAGIC
-- MAGIC This approach has two main advantages:
-- MAGIC * Managed tables in Unity Catalog can benefit from product optimization features that may not work well (if at all) on tables that aren't managed
-- MAGIC * Moving the data also gives you the opportunity to restructure your tables, in case you want to make any changes
-- MAGIC
-- MAGIC The main disadvantage to this approach is, particularly for large datasets, the time and cost associated with copying the data.
-- MAGIC
-- MAGIC In this section, we cover two different options that will move table data into the Unity Catalog metastore.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Cloning a table
-- MAGIC
-- MAGIC Cloning a table is optimal when the source table is Delta (see <a href="https://docs.databricks.com/delta/clone.html" target="_blank">documentation</a> for a full explanation). Run the following cell to check the format of the source table.

-- COMMAND ----------

DESCRIBE EXTENDED hive_metastore.${da.schema}.movies

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Referring to the *Provider*, we see the source is a Delta table. So let's clone the table now, creating a destination table named *movies_clone*.

-- COMMAND ----------

CREATE OR REPLACE TABLE ${da.catalog}.${da.schema}.movies_clone CLONE hive_metastore.${da.schema}.movies

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Create Table As Select (CTAS)
-- MAGIC
-- MAGIC Using CTAS is universally applicable. Let's copy the table using this approach, creating a destination table named *movies*.

-- COMMAND ----------

CREATE OR REPLACE TABLE ${da.catalog}.${da.schema}.movies_ctas
  AS SELECT * FROM hive_metastore.`${da.schema}`.movies

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Applying tranformations during the upgrade
-- MAGIC
-- MAGIC While migrating your datasets to Unity Catalog, it's a great time to consider your table structures and whether they still address your organization's business requirements that may have changed over time.
-- MAGIC
-- MAGIC The two examples we just saw take an exact copy of the source table, with clone being simpler to use and migrating additional metadata. But using a CTAS, we can easily perform any transformations during the upgrade. For example, let's expand on the previous example to do the following tranformations:
-- MAGIC * Assign the name *idx* to the first column
-- MAGIC * Additionally select only the columns *title*, *year*, *budget* and *rating*
-- MAGIC * Convert *year* and *budget* to **INT** (replacing any instances of the string *NA* with 0)
-- MAGIC * Convert *rating* to **DOUBLE**

-- COMMAND ----------

CREATE OR REPLACE TABLE ${da.catalog}.${da.schema}.movies_transformed
AS SELECT
  _c0 AS idx,
  title,
  CAST(year AS INT) AS year,
  CASE WHEN
    budget = 'NA' THEN 0
    ELSE CAST(budget AS INT)
  END AS budget,
  CAST(rating AS DOUBLE) AS rating
FROM hive_metastore.${da.schema}.movies

-- COMMAND ----------

SELECT * FROM ${da.catalog}.${da.schema}.movies_transformed

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Upgrading metadata only
-- MAGIC
-- MAGIC We have seen approaches that involve moving table data from wherever it is currently to the Unity Catalog metastore. However, in upgrading existing external tables, some may prefer to leave data in place. The main reasons to consider this approach might include:
-- MAGIC * Existing data location satisfies an internal or regulatory requirement of some sort
-- MAGIC * Existing data format is not Delta and must remain that way
-- MAGIC * Outsiders must be able to write to the data
-- MAGIC * For large datasets, there exists a strong preference to avoid the time and/or cost of moving the data
-- MAGIC
-- MAGIC Note the following constraints to this approach:
-- MAGIC
-- MAGIC * Source table must be an external table
-- MAGIC * There must be a storage credential referencing the storage container where the source table data resides
-- MAGIC
-- MAGIC In this section, we cover two different options that will upgrade to an external table without moving any table data.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Using SYNC
-- MAGIC
-- MAGIC The **`SYNC`** SQL command allows us to upgrade tables or entire schemas from the Hive metastore to Unity Catalog. Note that we're including the **`DRY RUN`** option, which is never a bad idea the first time we attempt to perform this operation; it will perform all the checks without attempting to create any new tables.

-- COMMAND ----------

SYNC TABLE ${da.catalog}.${da.schema}.test_external FROM hive_metastore.${da.schema}.test_external DRY RUN

-- COMMAND ----------

-- MAGIC %md
-- MAGIC There are a number of conditions that could cause this operation to fail. In this case, source table data resides in DBFS. This upgrade path via **`SYNC`** is currently not supported.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Using the Data Explorer
-- MAGIC
-- MAGIC Let's try upgrading the table using the Data Explorer user interface.
-- MAGIC
-- MAGIC 1. Open the **Data** page.
-- MAGIC 1. Select the *hive_metastore* catalog, then select the schema containing the source table.
-- MAGIC 1. Select the table *test_external* (the user interface will not allow you to upgrade the *movies* table since this upgrade path ony supports external tables).
-- MAGIC 1. Click **Upgrade**.
-- MAGIC 1. Select your destination catalog (either *main* or whichever catalog you chose during setup).
-- MAGIC 1. Select the schema that was created during setup.
-- MAGIC 1. For this example, let's leave owner set to the default and click **Next**.
-- MAGIC 1. Finally, let's click **Run upgrade**.
-- MAGIC
-- MAGIC Note that this will fail, because we do not have a storage credential set up for the storage location where the source table data resides. But the main purpose of this section was to become familiar with the workflow.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Clean up
-- MAGIC Run the following cell to remove the resources that were created in this example.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC da.cleanup()

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC &copy; 2023 Databricks, Inc. All rights reserved.<br/>
-- MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
-- MAGIC <br/>
-- MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
