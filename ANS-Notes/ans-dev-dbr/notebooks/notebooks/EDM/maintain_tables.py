# Databricks notebook source
# MAGIC %run ./functions

# COMMAND ----------

from delta.tables import *

# COMMAND ----------

for database_name in get_databases():
  print(database_name)
  for table_name in get_tables(database_name):
    try:      
      delta_table = DeltaTable.forName(spark, table_name)
      delta_table.vacuum(720)
    except Exception as e:
      print(e)

# COMMAND ----------


