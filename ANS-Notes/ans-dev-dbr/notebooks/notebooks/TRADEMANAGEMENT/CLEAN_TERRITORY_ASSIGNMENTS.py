# Databricks notebook source
# MAGIC %run ../_SHARED/FUNCTIONS/1.2/bootstrap

# COMMAND ----------

# MAGIC %run ../_SHARED/PARTIALS/1.2/header_s_trademanagement

# COMMAND ----------

# SETUP PARAMETERS
file_name = get_file_name(table_name, 'dlt')
target_file = get_file_path(temp_folder, file_name, target_container, target_storage)
print(file_name)
print(target_file)

# COMMAND ----------

# EXTRACT
main_f = spark.table('s_core.territory_assignments_agg')

# COMMAND ----------

# LOAD
main_f.write.format('delta').mode('overwrite').option('overwriteSchema', 'true').save(target_file)

# COMMAND ----------

dbutils.notebook.exit("Success")
