# Databricks notebook source
# MAGIC %run ../Includes/Classroom-Setup-05.2.4L

# COMMAND ----------

# DBTITLE 0,--i18n-f8f98f46-f3d3-41e5-b86a-fc236813e67e
# MAGIC %md
# MAGIC The **system** directory captures events associated with the pipeline.These event logs are stored as a Delta table. 
# MAGIC Let's query the table.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM delta.`${DA.paths.storage_location}/system/events`

# COMMAND ----------

# DBTITLE 0,--i18n-72eb29d2-cde0-4488-954b-a0ed47ead8eb
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC Let's query the gold table.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM ${DA.schema_name}.daily_patient_avg

# COMMAND ----------

DA.cleanup()

