-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC
-- MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #  Managing external storage
-- MAGIC
-- MAGIC In this lab you will learn how to:
-- MAGIC * Create a storage credential and external location
-- MAGIC * Control access to files using an external location
-- MAGIC * Create and manage access to external table
-- MAGIC * Compare attributes and behaviors of managed versus external tables
-- MAGIC * Use external locations to physically segregate data in your metastore

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Prerequisites
-- MAGIC
-- MAGIC If you would like to follow along with this lab, you must:
-- MAGIC * Have account admin capability in order to create a storage credential and external location
-- MAGIC * Have cloud resources (storage location and credentials for accessing the storage) to support an external location, which can be provided by your cloud administrator
-- MAGIC * Have workspace admin capability in order to create a new user, and to create and assign a new SQL warehouse

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Setup
-- MAGIC
-- MAGIC Run the following cell to perform some setup. In order to avoid conflicts in a shared training environment, this will create a uniquely named catalog with schema exclusively for your use.

-- COMMAND ----------

-- MAGIC %run ./Includes/Managing-external-storage-setup

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Create a secondary user for testing access control
-- MAGIC
-- MAGIC To test and observe the impact of the changes we'll make, we'll need a secondary user and group in our account. This requires account administrator capabilities.
-- MAGIC
-- MAGIC 1. Open the user menu and select **Manage Account**.
-- MAGIC 1. Go to the **User management** page.
-- MAGIC 1. In the **Users** tab, click **Add user**.
-- MAGIC 1. Provide an email address. Choose an email address that works with your identity setup and to which you have access. If you're free to choose any email address you like, feel free to use the one suggested in the table output above. This email address takes advantage of the free service, <a href="https://www.dispostable.com/" target="_blank">dispostable.com</a> for establishing a temporary, disposable email address.
-- MAGIC 1. Specify a first and last name, then click **Send invite**.
-- MAGIC 1. Now we'll enlist this user in a new group named *dbacademy_analysts*. In the **Groups** tab, click **Add group**.
-- MAGIC 1. Specify a **Group name** of *dbacademy_analysts*.
-- MAGIC 1. Click **Save**.
-- MAGIC 1. Now let's click **Add members**.
-- MAGIC 1. Search for and select the new user created in the previous section and click **Add**.
-- MAGIC
-- MAGIC Though we have a user defined at the account level, we also need to bring them over to the workspace so that they're able to process queries. Let's do that now.
-- MAGIC
-- MAGIC 1. Return to the Data Science and Engineering Workspace.
-- MAGIC 1. Open the user menu and select **Admin Settings**.
-- MAGIC 1. Go to the **User management** page.
-- MAGIC 1. In the **Users** tab, click **Add user**.
-- MAGIC 1. Locate and select the user we just created, and click **Add**. 
-- MAGIC
-- MAGIC Now let's create a SQL warehouse so this new user can process queries. If you already have such a warehouse created, you can skip creating the warehouse and log in as the secondary user as outlined below.
-- MAGIC
-- MAGIC 1. In the **SQL** persona, click the **SQL Warehouses** icon in the left sidebar.
-- MAGIC 1. Click **Create SQL warehouse**.
-- MAGIC 1. Assign a name.
-- MAGIC 1. Select a cluster size; let's choose *2X-Small* for economy.
-- MAGIC 1. Click **Create**.
-- MAGIC 1. In the **Manage permissions** box, add a new permission: *All Users* and *Can use*.
-- MAGIC
-- MAGIC At this point, the new user will have been issued an email inviting them to join and set their password. In a separate private/guest browser session, go to the inbox for the new user (if you used the suggested email address above, go to <a href="https://www.dispostable.com/" target="_blank">dispostable.com</a>), open the email, and follow the link to set up a password and access the workspace.
-- MAGIC
-- MAGIC 1. Once logged in, go to the **SQL** persona.
-- MAGIC 1. Open the **SQL Editor** page and click **Create query**.
-- MAGIC 1. Select the SQL warehouse that we created moments ago.
-- MAGIC 1. Return to this notebook and continue following along.
-- MAGIC
-- MAGIC To test and observe the impact of the changes we'll me making later in the lab, we'll be referring to this secondary browser session.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Creating a storage credential
-- MAGIC
-- MAGIC Storage credentials enable Unity Catalog to connect to external cloud storage locations and access files there, either for direct file access or for use with external tables. 
-- MAGIC
-- MAGIC We need a few cloud-specific resources in order to create one. Your cloud administrator can prepare them following these procedures:
-- MAGIC
-- MAGIC * <a href="https://docs.databricks.com/data-governance/unity-catalog/manage-external-locations-and-credentials.html" target="_blank">AWS documentation</a>
-- MAGIC * <a href="https://learn.microsoft.com/en-us/azure/databricks/data-governance/unity-catalog/manage-external-locations-and-credentials" target="_blank">Azure documentation</a>
-- MAGIC * <a href="https://docs.gcp.databricks.com/data-governance/unity-catalog/manage-external-locations-and-credentials.html" target="_blank">GCP documentation</a>
-- MAGIC
-- MAGIC Assuming we have the cloud resources we need, let's create the storage credential.
-- MAGIC
-- MAGIC 1. Open the **Data** page.
-- MAGIC 1. Open the **Storage Credentials** panel.
-- MAGIC 1. Let's click **Create credential**.
-- MAGIC 1. Now let's specify the name. This must be unique for storage credentials in the metastore. The setup we ran earlier generated a unique name, so we can copy that generated value from the table output.
-- MAGIC 1. Paste the cloud credential as obtained from your cloud administrator.
-- MAGIC 1. Let's click **Create**.
-- MAGIC
-- MAGIC Now let's look at the properties of the storage credential using SQL.

-- COMMAND ----------

DESCRIBE STORAGE CREDENTIAL ${da.storage_credential}

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Creating and managing external locations
-- MAGIC
-- MAGIC Storage credentials represent a connection to an external cloud storage container and support a set of privileges that allows us to do full access control. However, any privileges granted on a storage credential apply to the entire container.
-- MAGIC
-- MAGIC It's fairly typical to subdivide such containers into subtrees for different purposes. If we do this, then it's also desirable to be able to control access to each of these subdivisions - something storage credentials cannot do since they only apply to the entire thing.
-- MAGIC
-- MAGIC For this reason, we have external locations. External locations build on storage credentials and additionally specify a path within the referenced storage container, providing us with a construct that allows us to do access control at the level of files or folders. Let's create one now.
-- MAGIC
-- MAGIC 1. On the **Data** page, click **External Locations**.
-- MAGIC 1. Let's click **Create location**.
-- MAGIC 1. Now let's specify the name. Just as with storage credentials, this must be unique for external locations in the metastore. The setup we ran earlier generated a unique name, so we can copy that generated value from the table output, but append it with the suffix *_files*.
-- MAGIC 1. Paste the URL or path that fully qualifies the location you want to represent. This can be the top-level path in the storage container. Append the path with the subfolder */files*.
-- MAGIC 1. Let's select the storage credential representing the storage to which this external location pertains.
-- MAGIC 1. Let's click **Create**.
-- MAGIC
-- MAGIC We've created one external location, which we'll use to demonstrate file access. Now let's follow the procedure to create another external location, with the following changes:
-- MAGIC * Use the suffix *_schema* for the name
-- MAGIC * Use the suffix */schema* for the path
-- MAGIC
-- MAGIC Now let's look at the properties of the external locations using SQL.

-- COMMAND ----------

DESCRIBE EXTERNAL LOCATION ${da.external_location}_files

-- COMMAND ----------

DESCRIBE EXTERNAL LOCATION ${da.external_location}_schema

-- COMMAND ----------

-- MAGIC %md
-- MAGIC The *url* values from the output of these cells is significant; we'll need those soon. Let's run the following cell to record those in Python and Spark SQL variables.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC external_delta_table = spark.sql(f"DESCRIBE EXTERNAL LOCATION {da.external_location}_files").first()['url'] + '/test.delta'
-- MAGIC spark.conf.set("da.external_delta_table", external_delta_table)
-- MAGIC
-- MAGIC external_managed_override = spark.sql(f"DESCRIBE EXTERNAL LOCATION {da.external_location}_schema").first()['url']
-- MAGIC spark.conf.set("da.managed_override", external_managed_override)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Granting file access
-- MAGIC
-- MAGIC As a recap, external locations support a privilege set that allows us to govern access to the files stored in those locations. These privileges and their associated capabilities include:
-- MAGIC * **READ FILES**: directly read files from this location
-- MAGIC * **WRITE FILES**: directly write files to this location

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Let's put our external location to use by writing out some files that we will govern. We'll write the files with some Python code that:
-- MAGIC 1. Retrieves the *url* value for the external location (we talked about this value a few moments ago)
-- MAGIC 2. Extends the URL with a subdirectory named *test.delta*
-- MAGIC 3. Uses that location to write a simple dataset in Delta format.
-- MAGIC
-- MAGIC Since we are the data owner for the external location that covers this destination, we can write files without any additional privilege grants.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df = spark.range(20)
-- MAGIC (df.write
-- MAGIC    .format("delta")
-- MAGIC    .mode('overwrite')
-- MAGIC    .save(external_delta_table))

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Let's use the **`LIST`** statement to look at the files we just wrote out.

-- COMMAND ----------

LIST '${da.external_delta_table}'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Accessing files as a secondary user
-- MAGIC
-- MAGIC Momentarily, we're going to refer back to the session we set up at the beginning of the lab to test file access as a secondary user. But first, let's run the following cell to generate a fully qualified query statement that does the same **`LIST`** operation we just did.

-- COMMAND ----------

SELECT "LIST '${da.external_delta_table}'" AS Query

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Paste the output into the New query tab of the DBSQL session and run it. It won't work yet, because there is no privilege on the external location. Let's grant the appropriate privilege now.

-- COMMAND ----------

GRANT READ FILES ON EXTERNAL LOCATION ${da.external_location}_files TO `dbacademy_analysts`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Repeat the query in the DBSQL environment, and with this grant in place the operation will succeed.
-- MAGIC
-- MAGIC Notice that we haven't had to deal with any schema or catalog privileges yet. We're dealing with storage credentials and external locations, and while these are both securable objects that live in the metastore, they sit off on their own outside the data object hierarchy. But this will change when we start working with external tables next.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Managing external tables
-- MAGIC
-- MAGIC Another important application of storage credentials and external locations is to support external tables; that is, tables whose data resides in external storage.
-- MAGIC
-- MAGIC Let's create an external table based on the delta files created previously. The syntax for creating an external table is similar to a managed table, with the addition of a **`LOCATION`** that specifies the full cloud storage path containing the data files backing the table.
-- MAGIC
-- MAGIC Keep in mind that when creating external tables, two conditions must be met:
-- MAGIC * We must have the **`CREATE EXTERNAL TABLE`** privilage on the external location or storage credential. But since we created both and are the data owner, no additional permissions are necessary.
-- MAGIC * We must have appropriate permissions on the schema and catalog in which we are creating the table.

-- COMMAND ----------

CREATE TABLE test_external
  LOCATION '${da.external_delta_table}'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Let's click the **Data** icon in the left sidebar to witness the effect of the above statement. We see that there is a new table contained within the *external_storage* schema.
-- MAGIC
-- MAGIC Let's quickly examine the contents of the table. As the data onwer, this query runs without issue.

-- COMMAND ----------

SELECT * FROM test_external

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Granting access to an external table
-- MAGIC
-- MAGIC Once an external table is created, access control works the same way as it does for a managed table. Let's see this in action now.
-- MAGIC
-- MAGIC Let's run the following cell to output a query statement that reads the external table. Copy and paste the output into a new query within the DBSQL environment, and run the query.

-- COMMAND ----------

SELECT "SELECT * FROM ${da.catalog}.${da.schema}.test_external" AS Query

-- COMMAND ----------

-- MAGIC %md
-- MAGIC This operation fails, even though moments ago, there were no issues reading the underlying files. Why? Because now we're querying this as a table, and now we're subject to the security model for tables, which requires:
-- MAGIC * **SELECT** on the table
-- MAGIC * **USE SCHEMA** on the schema
-- MAGIC * **USE CATALOG** on the catalog
-- MAGIC
-- MAGIC Let's address this now with the following three **`GRANT`** statements.

-- COMMAND ----------

GRANT SELECT ON TABLE test_external TO `dbacademy_analysts`;
GRANT USE SCHEMA ON SCHEMA ${da.schema} TO `dbacademy_analysts`;
GRANT USE CATALOG ON CATALOG ${da.catalog} TO `dbacademy_analysts`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Re-run the query in DBSQL, and now this will succeed.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Comparing managed and external tables
-- MAGIC
-- MAGIC Let's compare managed and external tables.
-- MAGIC
-- MAGIC First, let's create a managed copy of the external table using **`CREATE TABLE AS SELECT`**.

-- COMMAND ----------

CREATE TABLE test_managed AS
  SELECT * FROM test_external

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Let's examine the contents of *test_managed*. The resuls are identical to those earlier when we queried *test_external*, since the table data was copied.

-- COMMAND ----------

SELECT * FROM test_managed

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Now let's do a comparison of the two tables using **`DESCRIBE EXTENDED`**.

-- COMMAND ----------

DESCRIBE EXTENDED test_managed

-- COMMAND ----------

DESCRIBE EXTENDED test_external

-- COMMAND ----------

-- MAGIC %md
-- MAGIC For the most part, the tables are similar, though there are some difference, particularly in *Type* and *Location*.
-- MAGIC
-- MAGIC We all know that if we drop a managed table like *test_managed* and recreate it (without using a **`CREATE TABLE AS SELECT`** statement, of course) the new table will be empty. But, let's try it out to see. First, let's run the following to obtain the command needed to recreate *test_managed*.

-- COMMAND ----------

SHOW CREATE TABLE test_managed

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Now let's drop the managed table.

-- COMMAND ----------

DROP TABLE test_managed

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Let's recreate the table. You can either run the code as it is below or replace it entirely with the equivalent code from *createtab_stmt* output above.

-- COMMAND ----------

CREATE TABLE ${da.catalog}.${da.schema}.test_managed (id BIGINT)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Now let's look at the contents.

-- COMMAND ----------

SELECT * FROM test_managed

-- COMMAND ----------

-- MAGIC %md
-- MAGIC It shouldn't come as a surprise that the new table is empty.
-- MAGIC
-- MAGIC Now let's now try this for our external table, *test_external*. Let's drop it.

-- COMMAND ----------

DROP TABLE test_external

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Let's repeat the command from earlier to recreate *test_external*.

-- COMMAND ----------

CREATE TABLE test_external
  LOCATION '${da.external_delta_table}'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Now let's look at the contents of the table.

-- COMMAND ----------

SELECT * FROM test_external

-- COMMAND ----------

-- MAGIC %md
-- MAGIC We see here, the data persisted because the underlying data files remained intact after the table was dropped. This demonstrates a key difference in behavior between managed and external tables. With a managed table, data is discarded along with the metadata when the table is dropped. But with an external table, the data is left alone since it is unmanaged. Therefore it is retained when you recreate the table using the same location.
-- MAGIC
-- MAGIC This is an important security consideration when dropping external tables; the files aren't automatically expunged. To eliminate the files, you'd need a process outside of Databricks to clean these up.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Segregating data
-- MAGIC
-- MAGIC Some organizations are subject to business or regulatory requirements specifying that data must be physically segregated at the infrastructure level. Satisfying such requirements often requires spreading data across separate storage containers. While external tables can accomplish this, it's a per-table approach that could quickly become cumbersome and difficult to maintain when we're talking about hundreds or thousands of tables.
-- MAGIC
-- MAGIC To simplify this use case, Databricks allows you to override the default location of managed table data at the schema and catalog levels. By default, data is maintained in the metastore bucket, but this override can redirect managed table data to an external location of your choosing.
-- MAGIC
-- MAGIC This can be specified when creating new catalogs or schemas in the Data Explorer user interface, or using the **`MANAGED LOCATION`** option when using SQL.
-- MAGIC
-- MAGIC Let's create a new schema which stores its managed data at the location controlled by the second external location created earlier. Note that since we're the owner of that external location object, no extra privileges are necessary. If that were not the case, we'd need the **`CREATE MANAGED STORAGE`** privilege on that external lation.

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS override_managed
  MANAGED LOCATION '${da.managed_override}'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC As we did earlier in the *Comparing managed and external tables* section, let's create a managed table in the new schema, modelled after the *test_external* table.

-- COMMAND ----------

CREATE TABLE override_managed.test_managed AS
  SELECT * FROM test_external

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Also as we did previously, let's use **`DESCRIBE EXTENDED`** to compare the tables.

-- COMMAND ----------

DESCRIBE EXTENDED test_managed

-- COMMAND ----------

DESCRIBE EXTENDED override_managed.test_managed

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Note from the *Type* value that both tables are managed, but the *Location* value differs. Without the override, we see the table data files stored in the standard metastore location, while the table with the override is being stored in a location managed by the external location.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Clean up
-- MAGIC Run the following cell to remove the catalog that was created, along with the storage credential and external location.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC da.cleanup()

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC &copy; 2023 Databricks, Inc. All rights reserved.<br/>
-- MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
-- MAGIC <br/>
-- MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
