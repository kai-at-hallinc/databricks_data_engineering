# Databricks notebook source
import re

class DBAcademyHelper():
     def __init__(self):
        import re
        
        # Do not modify this pattern without also updating the Reset notebook.
        username = spark.sql("SELECT current_user()").first()[0]
        clean_username = re.sub("[^a-zA-Z0-9]", "_", username)
        self.catalog = f"dbacademy_{clean_username}"        
        spark.conf.set("da.catalog", self.catalog)
        spark.conf.set("da.secondary_user", f"{clean_username}@dispostable.com")

da = DBAcademyHelper()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT col1 AS `Object`,col2 AS `Name`
# MAGIC FROM VALUES
# MAGIC   ('Catalog','${da.catalog}'),
# MAGIC   ('Secondary user','${da.secondary_user}')
