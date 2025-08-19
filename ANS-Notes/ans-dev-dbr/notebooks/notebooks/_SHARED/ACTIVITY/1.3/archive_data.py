# Databricks notebook source
# MAGIC %run ../../FUNCTIONS/1.3/bootstrap

# COMMAND ----------

# INPUT PARAMETERS
archive = get_input_param('archive')
database_name = get_input_param('database_name')
key_columns = get_input_param('key_columns')
table_name = get_input_param('table_name')
target_folder = get_input_param('target_folder')

# COMMAND ----------

if archive is None:
  dbutils.notebook.exit(True)

# COMMAND ----------

# SETUP VARIABLES
source_table = get_table_name(database_name, table_name)
print('source_table: ' + source_table)

# COMMAND ----------

# READ
main = spark.table(source_table)

# COMMAND ----------

# TRANSFORM
main_f = main.transform(attach_archive_column(archive_type = archive))
main_f.cache()
main_f.display()

# COMMAND ----------

# LOAD
target_table = get_table_name(database_name, table_name + '_arch_' + archive)
register_hive_table(main_f, target_table, target_folder, options = {'partition_column': '_ARCHIVE'})
merge_into_table(main_f, target_table, key_columns + ',_ARCHIVE', options = {'auto_merge': True})

# COMMAND ----------


