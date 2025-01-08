-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC
-- MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Creating and governing data objects
-- MAGIC
-- MAGIC In this lab you will learn how to:
-- MAGIC * Create and manage data objects like catalogs, schemas and tables using SQL and the Data Explorer
-- MAGIC * Manage access to these objects
-- MAGIC * Use dynamic views to protect columns and rows within tables

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Prerequisites
-- MAGIC
-- MAGIC If you would like to follow along with this lab, you must:
-- MAGIC * Have metastore admin capability or **`CREATE CATALOG`** privileges in order to create a catalog
-- MAGIC * Have workspace admin capability in order to create a new user, and to create and assign a new SQL warehouse

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Three-level namespace recap
-- MAGIC
-- MAGIC Anyone with SQL experience will likely be familiar with the traditional two-level namespace to address tables or views within a schema (often referred to as a database) as shown in the following example:
-- MAGIC
-- MAGIC     SELECT * FROM myschema.mytable;
-- MAGIC
-- MAGIC Unity Catalog introduces the concept of a **catalog** into the hierarchy, which provides another containment layer above the schema layer. This provides a new way for organizations to segragate their data and can be handy in many use cases. For example:
-- MAGIC
-- MAGIC * Separating data relating to business units within your organization (sales, marketing, human resources, etc)
-- MAGIC * Satisfying SDLC requirements (dev, staging, prod, etc)
-- MAGIC * Establishing sandboxes containing temporary datasets for internal use
-- MAGIC
-- MAGIC You can have as many catalogs as you want in the metastore, and each can contain as many schemas as you want. To deal with this additional level, complete table/view references in Unity Catalog use a three-level namespace, like this:
-- MAGIC
-- MAGIC     SELECT * FROM mycatalog.myschema.mytable;
-- MAGIC     
-- MAGIC We can take advantage of the **`USE`** statements to select a default catalog or schema to make our queries easier to write and read:
-- MAGIC
-- MAGIC     USE CATALOG mycatalog;
-- MAGIC     USE SCHEMA myschema;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Setup
-- MAGIC
-- MAGIC Let's run the following cell to perform some setup. In order to avoid conflicts in a shared training environment, this will generate a unique catalog name exclusively for your use, which we will use shortly.

-- COMMAND ----------

-- MAGIC %run ./Includes/Creating-governing-data-setup

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Populating the metastore
-- MAGIC
-- MAGIC In this section, let's explore how to create data containers and objects in the metastore. This can using SQL. Note that the SQL statements used throughout this lab could also be applied in DBSQL as well.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Creating a catalog
-- MAGIC
-- MAGIC We'll begin by creating a new catalog in our metastore, which represents the first component of the three-level namespace.
-- MAGIC
-- MAGIC We'll use the variable **`${da.catalog}`**, which was created and displayed during setup. This variable provides a name that is guaranteed to be unique in the metastore. This is useful if you have many folks going through this training material. If you don't need this, you can replace the string **`${da.catalog}`** with any name you like.

-- COMMAND ----------

CREATE CATALOG IF NOT EXISTS ${da.catalog}

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Selecting a default catalog
-- MAGIC
-- MAGIC Let's select the newly created catalog as the default. Now, any schema references will be assumed to be in this catalog unless explicitly overridden by a catalog reference.

-- COMMAND ----------

USE CATALOG ${da.catalog}

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Creating a schema
-- MAGIC
-- MAGIC The concept of a schema is unchanged by Unity Catalog. Schemas contain data objects like tables, views, and user-defined functions. With a new catalog established, let's create a new schema. For the name, we don't need to go to any effort to generate a unique name like we did for the catalog, since we are now in a brand new catalog that is isolated from everything else in the metastore.

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS example

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Selecting a default schema
-- MAGIC
-- MAGIC Let's select the newly created schema as the default. Now, any data references will be assumed to be in the catalog and schema we created, unless explicitely overridden by a two- or three-level reference.

-- COMMAND ----------

USE SCHEMA example

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Creating data objects
-- MAGIC With all the necessary containment in place, let's turn our attention to creating some data objects within a schema.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Tables
-- MAGIC
-- MAGIC First let's create a table. For this example, we'll pre-populate the table with a simple mock dataset containing synthetic patient heart rate data.
-- MAGIC
-- MAGIC Note the following:
-- MAGIC * We only need to specify the table name when creating the table. We don't need to specify the containing catalog or schema because we already selected defaults earlier.
-- MAGIC * This will be a managed table, because we aren't specifying a **`LOCATION`**.
-- MAGIC * Because it's a managed table, it will also be a Delta table.

-- COMMAND ----------

CREATE OR REPLACE TABLE silver
(
  device_id  INT,
  mrn        STRING,
  name       STRING,
  time       TIMESTAMP,
  heartrate  DOUBLE
);

INSERT INTO silver VALUES
  (23,'40580129','Nicholas Spears','2020-02-01T00:01:58.000+0000',54.0122153343),
  (17,'52804177','Lynn Russell','2020-02-01T00:02:55.000+0000',92.5136468131),
  (37,'65300842','Samuel Hughes','2020-02-01T00:08:58.000+0000',52.1354807863),
  (23,'40580129','Nicholas Spears','2020-02-01T00:16:51.000+0000',54.6477014191),
  (17,'52804177','Lynn Russell','2020-02-01T00:18:08.000+0000',95.033344842),
  (37,'65300842','Samuel Hughes','2020-02-01T00:23:58.000+0000',57.3391541312),
  (23,'40580129','Nicholas Spears','2020-02-01T00:31:58.000+0000',56.6165053697),
  (17,'52804177','Lynn Russell','2020-02-01T00:32:56.000+0000',94.8134313932),
  (37,'65300842','Samuel Hughes','2020-02-01T00:38:54.000+0000',56.2469995332),
  (23,'40580129','Nicholas Spears','2020-02-01T00:46:57.000+0000',54.8372685558);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Views
-- MAGIC
-- MAGIC Let's create a *gold* view that presents a processed view of the *silver* table data by averaging heart rate data per patient on a daily basis.

-- COMMAND ----------

CREATE OR REPLACE VIEW gold AS (
  SELECT mrn, name, MEAN(heartrate) avg_heartrate, DATE_TRUNC("DD", time) date
  FROM silver
  GROUP BY mrn, name, DATE_TRUNC("DD", time))

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### User-defined functions
-- MAGIC
-- MAGIC User-defined functions are managed within schemas as well. For this example, we'll set up simple function that masks all but the last two characters of a string.

-- COMMAND ----------

CREATE OR REPLACE FUNCTION dbacademy_mask(x STRING)
  RETURNS STRING
  RETURN CONCAT(REPEAT("*", LENGTH(x) - 2), RIGHT(x, 2))

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Let's see how the function we just defined works.

-- COMMAND ----------

SELECT dbacademy_mask('sensitive data') AS data

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Exploring the metastore
-- MAGIC
-- MAGIC In this section, let's explore our metastore and its data objects.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### SQL
-- MAGIC Let's explore objects using the SQL commands. Though we embed them in a notebook here, you can easily port them over for execution in the DBSQL environment as well.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### SHOW
-- MAGIC
-- MAGIC We use the SQL **`SHOW`** command to inspect elements at various levels in the hierarchy. Let's start by taking stock of the catalogs in the metastore.

-- COMMAND ----------

SHOW CATALOGS

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Do any of these entries surprise you? You should definitely see a catalog beginning with the prefix *dbacademy_*, which is the one we created earlier. But there may be more, depending on the activity in your metastore, and how your workspace is configured. In addition to catalogs others have created, you may also see:
-- MAGIC * *hive_metastore*. This is not actually a catalog. Rather, it's Unity Catalog's way of making the workspace local Hive metastore seamlessly accessible through the three-level namespace.
-- MAGIC * *main*: this catalog is created by default with each new metastore.
-- MAGIC * *samples*: this references a cloud container containining sample datasets hosted by Databricks.
-- MAGIC * *system*: this catalog provides an interface to the system tables - a collection of views that return information about objects across all catalogs in the metastore.
-- MAGIC
-- MAGIC Now let's take a look at the schemas contained in the catalog we created. Note that the **`IN ${da.catalog}`** part isn't strictly necessary here since we selected it as a default catalog earlier, but we include it here to be explicit.

-- COMMAND ----------

SHOW SCHEMAS IN ${da.catalog}

-- COMMAND ----------

-- MAGIC %md
-- MAGIC The *example* schema, of course, is the one we created earlier but there are a couple additional entries you maybe weren't expecting:
-- MAGIC * *default*: this schema is created by default with each new catalog.
-- MAGIC * *information_schema*: this schema is also created by default with each new catalog and provides a set of views describing the objects in the catalog.
-- MAGIC
-- MAGIC Now let's take a look at the tables contained in the schema we created.

-- COMMAND ----------

SHOW TABLES IN ${da.catalog}.example

-- COMMAND ----------

-- MAGIC %md
-- MAGIC There's a command available for exploring all the different object types. For example, we can display the available functions. This will include all functions available in the namespace, including built-in functions.

-- COMMAND ----------

SHOW FUNCTIONS IN ${da.catalog}.example

-- COMMAND ----------

-- MAGIC %md
-- MAGIC We can also use **`SHOW`** to see privileges granted on data objects.

-- COMMAND ----------

SHOW GRANTS ON TABLE silver

-- COMMAND ----------

-- MAGIC %md
-- MAGIC There are no grants on this table currently. Only we, the data owner, can access this table. We'll get to granting privileges shortly.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### DESCRIBE
-- MAGIC
-- MAGIC We also have **`DESCRIBE`** at our disposal, to provide additional information about a specific object.

-- COMMAND ----------

DESCRIBE TABLE ${da.catalog}.example.silver

-- COMMAND ----------

DESCRIBE TABLE ${da.catalog}.example.gold

-- COMMAND ----------

DESCRIBE FUNCTION ${da.catalog}.example.dbacademy_mask

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### System catalog
-- MAGIC The *system* catalog provides an interface to the system tables; that is a collection of views whose purpose is to provide a SQL-based, self-describing API to the metadata related to objects across all catalogs in the metastore. This exposes a host of information useful for administration and housekeeping and there are a lot of applications for this.
-- MAGIC
-- MAGIC Let's consider the following example, which shows all tables that have been modified in the last 24 hours.

-- COMMAND ----------

SELECT table_name, table_owner, created_by, last_altered, last_altered_by, table_catalog
    FROM system.information_schema.tables
    WHERE  datediff(now(), last_altered) < 1;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Information schema
-- MAGIC
-- MAGIC The *information_schema*, automatically created with each catalog, contains a collection of views whose purpose is to provide a SQL-based, self-describing API to the metadata related to the elements contained within the catalog.
-- MAGIC
-- MAGIC The relations found in this schema are documented <a href="https://docs.databricks.com/sql/language-manual/sql-ref-information-schema.html" target="_blank">here</a>. As a basic example, let's see all the tables in the catalog:

-- COMMAND ----------

SELECT * FROM ${da.catalog}.information_schema.tables

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Data explorer
-- MAGIC Let's click the **Data** icon in the left sidebar to explore the metastore using the Data Explorer user interface.
-- MAGIC * Observe the catalogs listed in the **Data** pane. The items in this list resemble those from the **`SHOW CATALOGS`** SQL statement we executed earlier.
-- MAGIC * Expand the item representing the catalog we created, then expand *example*. This displays a list of tables/views.
-- MAGIC * Select *gold* to see detailed information regarding the view. From here, we can see the schema, sample data, details, and permissions (which we'll get to shortly).

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Lineage
-- MAGIC
-- MAGIC Data lineage is a key pillar of any data governance solution. In the **Lineage** tab, we can identify elements that are related to the selected object:
-- MAGIC * With **Upstream** selected, we see objects that gave rise to this object, or that this object uses. This is useful for tracing the source of your data.
-- MAGIC * With **Downstream** selected, we see objects that are using this object. This is useful for performing impact analyses.
-- MAGIC * The lineage graph provides a visualization of the lineage relationships.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Controlling access to data
-- MAGIC
-- MAGIC In this section we're going to configure permissions on data objects we created. 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Setup
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
-- MAGIC To test and observe the impact of the changes we'll me making in this section, we'll be referring to this secondary browser session.
-- MAGIC
-- MAGIC Now let's run the following cell to generate a fully qualified query statement to paste as a new query into the DBSQL session.

-- COMMAND ----------

SELECT "SELECT * FROM ${da.catalog}.example.gold" AS Query

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Paste the output into the **New query** tab of the DBSQL session and run it. Naturally, this will fail since we haven't granted access yet. Let's address this now.
-- MAGIC
-- MAGIC Unity catalogs's security model essentially accomodates for two approaches to managing permissions over your data objects:
-- MAGIC
-- MAGIC 1. Assigning permissions en masse by taking advantage of Unity Catalog's privilege inheritance.
-- MAGIC 1. Explicitly assigning permissions to specific objects. This model is quite secure, but involves more work to set up and administer.
-- MAGIC
-- MAGIC We'll explore both approaches to provide an understanding of how each one works.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Inherited privileges
-- MAGIC
-- MAGIC By default, no permissions are implied by the metastore. In order to access any data objects, users need appropriate permissions for the data object in question (a view, in this case), as well as all containing elements (the schema and catalog).
-- MAGIC
-- MAGIC As we've seen, securable objects in Unity Catalog are hierarchical, and privileges are inherited downward. Using this property makes it easy to set up default access rules for your data. Using privilege inheritance, let's build a permission chain that will allow the analyst user to access the *gold* view.

-- COMMAND ----------

GRANT USE CATALOG,USE SCHEMA,SELECT ON CATALOG ${da.catalog} TO `dbacademy_analysts`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Attempt to run the query in DBSQL again. This time, the query succeeds because all the appropriate permissions are in place.
-- MAGIC
-- MAGIC This seems simple enough; one single statement is all it takes. But there are some very important things to keep in mind with this approach:
-- MAGIC
-- MAGIC * The grantee now has the **`SELECT`** privilege on **all** applicable objects (that is, tables and views) in **all** schemas within the catalog
-- MAGIC * This privilege will also be extended to any new tables/views, as well as any new schemas that appear in the future
-- MAGIC
-- MAGIC While this can be very convenient (for example, when you'd like users to have access to hundreds or thousands of tables) we must be very careful how we set this up when using privilege inheritance because it's much easier to grant permissions to the wrong things accidentally. Also keep in mind the above statement is extreme. A slightly less permissive arrangement can be made, while still leveraging privilege inheritance, with the following two grants. Basically, this pushes the **`USE SCHEMA`** and **`SELECT`** down a level, so that grantees only have access to all applicable objects in the *example* schema.
-- MAGIC
-- MAGIC Note, you don't need to run these statements; they're merely provided as an example to illustrate the different types of privilege structures you can create that take advantage of inheritance.
-- MAGIC ```
-- MAGIC GRANT USE CATALOG ON CATALOG ${da.catalog} TO `dbacademy_analysts`;
-- MAGIC GRANT USE SCHEMA,SELECT ON CATALOG ${da.catalog}.example TO `dbacademy_analysts`
-- MAGIC ```

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Revoking privileges
-- MAGIC
-- MAGIC No data governance platform would be complete without the ability to revoke previously issued grants. In preparation for testing the next approach to granting privileges, let's unwind what we just did using **`REVOKE`**.

-- COMMAND ----------

REVOKE USE CATALOG,USE SCHEMA,SELECT ON CATALOG ${da.catalog} FROM `dbacademy_analysts`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Explicit privileges
-- MAGIC
-- MAGIC Using explicit privilege grants, let's build a permission chain that will allow the analyst user to access the *gold* view.

-- COMMAND ----------

GRANT USE CATALOG ON CATALOG ${da.catalog} TO `dbacademy_analysts`;
GRANT USE SCHEMA ON SCHEMA ${da.catalog}.example TO `dbacademy_analysts`;
GRANT SELECT ON VIEW ${da.catalog}.example.gold TO `dbacademy_analysts`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC With these grants in place, attempt to run the query in DBSQL again. This time, the query still succeeds because all the appropriate permissions are in place; we've just taken a very different approach to establishing them.
-- MAGIC
-- MAGIC This seems more complicated, and it is. One statement becomes three, and this only provides access to a single view. In this scheme, we'd have to do an additional **`SELECT`** grant for each additional table or view we wanted to permit. But this complication comes with the benefit of security. Now, our analyst can access the *gold* view, but nothing else. And there's no chance they could accidentally get access to some other random object due to what we've done here. So this is very expicit and secure, but one can imagine it would be very cumbersome when dealing with lots of tables and views.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Views versus tables
-- MAGIC
-- MAGIC We've explored two different approaches to managing permissions, and we now have permissions configured such that our user can access the *gold* view, which processes and displays data from the *silver* table. But let's attempt to directly access the *silver* table as our user. In DBSQL, create a new query that's a copy of the existing query, but replace *gold* with *silver* and run it.
-- MAGIC
-- MAGIC With explicit privileges in place, the query fails as it should. Why then, does the query against the *gold* view work? Because the view's owner has appropriate privileges on the *silver* table (through data ownership). This property gives rise t interesting applications of views in table security, which we cover in the next section.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Protecting columns and rows with dynamic views
-- MAGIC
-- MAGIC We have seen that Unity Catalog's treatment of views provides the ability for views to protect access to tables; users can be granted access to views that manipulate, transform, or obscure data from a source table, without needing to provide direct access to the source table.
-- MAGIC
-- MAGIC Dynamic views provide the ability to do fine-grained access control of columns and rows within a table, conditional on the principal running the query. Dynamic views are an extension to standard views that allow us to do things like:
-- MAGIC * partially obscure column values or redact them entirely
-- MAGIC * omit rows based on specific criteria
-- MAGIC
-- MAGIC Access control with dynamic views is achieved through the use of functions within the definition of the view. These functions include:
-- MAGIC * **`current_user()`**: returns the email address of the user querying the view
-- MAGIC * **`is_account_group_member()`**: returns TRUE if the user querying the view is a member of the specified group
-- MAGIC * **`is_member()`**: returns TRUE if the user querying the view is a member of the specified workspace-local group
-- MAGIC
-- MAGIC Note: Databricks generally advises against using the **`is_member()`** function in production, since it references workspace-local groups and hence introduces a workspace dependency into a metastore that potentially spans multiple workspaces.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Redacting columns
-- MAGIC
-- MAGIC Suppose we want everyone to be able to see aggregated data trends from the *gold* view, but we don't want to disclose patient PII to everyone. Let's redefine the view to redact the *mrn* and *name* columns, so that only members of *dbacademy_analysts* can see it, using the **`is_account_group_member()`** function.

-- COMMAND ----------

CREATE OR REPLACE VIEW gold AS
SELECT
  CASE WHEN
    is_account_group_member('dbacademy_analysts') THEN mrn 
    ELSE 'REDACTED'
  END AS mrn,
  CASE WHEN
    is_account_group_member('dbacademy_analysts') THEN name
    ELSE 'REDACTED'
  END AS name,
  MEAN(heartrate) avg_heartrate,
  DATE_TRUNC("DD", time) date
  FROM silver
  GROUP BY mrn, name, DATE_TRUNC("DD", time);

-- Re-issue the grant --
GRANT SELECT ON VIEW gold TO `dbacademy_analysts`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Let's query the view.

-- COMMAND ----------

SELECT * FROM gold

-- COMMAND ----------

-- MAGIC %md
-- MAGIC If run as a user that's not a member of *dbacademy_analsysts*, the *mrn* and *name* columns are redacted. Now, revisit the DBSQL session and rerun the query on the *gold* view. We will see unfiltered output.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Restrict rows
-- MAGIC
-- MAGIC Now let's suppose we want a view that, rather than aggregating and redacting columns, simply filters out rows from the source. Let's  apply the same **`is_account_group_member()`** function to create a view that passes through only rows whose *device_id* is less than 30. Row filtering is done by applying the conditional as a **`WHERE`** clause.

-- COMMAND ----------

CREATE OR REPLACE VIEW gold AS
SELECT
  mrn,
  time,
  device_id,
  heartrate
FROM silver
WHERE
  CASE WHEN
    is_account_group_member('dbacademy_analysts') THEN TRUE
    ELSE device_id < 30
  END;

-- Re-issue the grant --
GRANT SELECT ON VIEW gold TO `dbacademy_analysts`

-- COMMAND ----------

SELECT * FROM gold

-- COMMAND ----------

-- MAGIC %md
-- MAGIC For us, three records are missing. Those records contained values for *device_id* that were caught by the filter. Now, revisit the DBSQL session and rerun the query on the *gold* view. We will see all ten records.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Data masking
-- MAGIC One final use case for dynamic views is data masking, or partially obscuring data. In the first example, we redacted columns entirely. Masking is similar in principle except we are displaying some of the data rather than replacing it entirely. And for this simple example, we'll leverage the *dbacademy_mask()* user-defined function that we created earlier to mask the *mrn* column, though SQL provides a fairly comprehensive library of built-in data manipulation functions that can be leveraged to mask data in a number of different ways. It's good practice to leverage those when you can.

-- COMMAND ----------

CREATE OR REPLACE VIEW gold AS
SELECT
  CASE WHEN
    is_account_group_member('dbacademy_analysts') THEN mrn
    ELSE dbacademy_mask(mrn)
  END AS mrn,
  time,
  device_id,
  heartrate
FROM silver
WHERE
  CASE WHEN
    is_account_group_member('dbacademy_analysts') THEN TRUE
    ELSE device_id < 30
  END;

-- Re-issue the grant --
GRANT SELECT ON VIEW gold TO `dbacademy_analysts`

-- COMMAND ----------

SELECT * FROM gold

-- COMMAND ----------

-- MAGIC %md
-- MAGIC For us, all values in the *mrn* column will be masked. Now, revisit the Databricks SQL session and rerun the query on the *gold* view as an analyst, and we'll see ten undisturbed records.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Clean up
-- MAGIC Let's run the following cell to remove the catalog that we created earlier. The **`CASCADE`** qualifier will remove the catalog along with any contained elements.

-- COMMAND ----------

DROP CATALOG IF EXISTS ${da.catalog} CASCADE

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC &copy; 2023 Databricks, Inc. All rights reserved.<br/>
-- MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
-- MAGIC <br/>
-- MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
