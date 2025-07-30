# Databricks notebook source
dbutils.fs.ls('dbfs:/mnt/dbacademy-datasets/data-engineer-learning-path/v02/healthcare')

# COMMAND ----------

# DBTITLE 1,install cli
# MAGIC %pip install databricks-cli

# COMMAND ----------

# DBTITLE 1,create databricks config
import os

token = dbutils.secrets.get('cli', 'token')
host = dbutils.secrets.get('cli', 'host')
serverless_compute_id = dbutils.secrets.get('cli', 'serverless_compute_id')
profile = dbutils.secrets.get('cli', 'profile')

# Set environment variables for Databricks CLI
os.environ['DATABRICKS_HOST'] = host
os.environ['DATABRICKS_TOKEN'] = token
os.environ['DATABRICKS_CONFIG_PROFILE'] = profile

# Create the .databrickscfg file with the profile
config_content = f"""
[{profile}]
host = {host}
token = {token}
"""

config_path = os.path.expanduser("~/.databrickscfg")

with open(config_path, "w") as config_file:
    config_file.write(config_content)
