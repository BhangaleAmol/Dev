# Databricks notebook source
# MAGIC %run ../../FUNCTIONS/1.1/bootstrap

# COMMAND ----------

# EXTRACT INPUT PARAMETERS
dbutils.widgets.removeAll()
input_params = get_input_params(['source_folder', 'database_name'])
item = get_input_param_json('item')
params = {**input_params, **item}
format_default_params(params)

# COMMAND ----------

# INPUT PAREMETERS
source_folder = get_param(params, "source_folder")
database_name = get_param(params, "database_name")
table_name = get_param(params, "table_name")
key_columns = get_param(params, "key_columns")
handle_delete = get_param(params, "handle_delete", "bool", False)
delete_type = get_param(params, "delete_type", "string", "soft_delete")
print_dict(params)

# COMMAND ----------

if not handle_delete:
  dbutils.notebook.exit(True)

# COMMAND ----------

# VALIDATE INPUT
if source_folder is None or database_name is None or table_name is None or key_columns is None:
  raise Exception("Input parameters are missing")

# COMMAND ----------

# SETUP VARIABLES
source_file_path = DATALAKE_ENDPOINT + source_folder + '/' + table_name + '.par'
print(source_file_path)

# COMMAND ----------

# READ KEYS
df = spark.read.format('parquet').load(source_file_path)
df.count()

# COMMAND ----------

# APPLY SOFT DELETE
if handle_delete and delete_type == "soft_delete":
  apply_soft_delete(df, flag_name = '_DELETED')

# COMMAND ----------

# APPLY HARD DELETE
if handle_delete and delete_type == "hard_delete":
  apply_hard_delete(df)

# COMMAND ----------

dbutils.notebook.exit(True)
