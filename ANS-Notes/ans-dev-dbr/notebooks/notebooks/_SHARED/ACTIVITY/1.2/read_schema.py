# Databricks notebook source
dbutils.widgets.text("database_name", "","")
database_name = dbutils.widgets.get("database_name")

dbutils.widgets.text("table_name", "","")
table_name = dbutils.widgets.get("table_name")

# COMMAND ----------

f_table_name = f'{database_name}.{table_name}'

# COMMAND ----------

import json

schema_list = spark.sql(f'DESCRIBE TABLE {f_table_name}').collect()
split_index = [r.col_name for r in schema_list].index('')
schema_dict = {r.col_name: r.data_type for idx, r in enumerate(schema_list) if idx < split_index}
dbutils.notebook.exit(json.dumps({"result": schema_dict}))
