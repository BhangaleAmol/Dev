# Databricks notebook source
# MAGIC %run ../_SHARED/FUNCTIONS/1.3/bootstrap

# COMMAND ----------

# MAGIC %run ./_SHARED/func_int_user

# COMMAND ----------

# INPUT
key_columns = get_input_param('key_columns')
source_folder = get_input_param('source_folder')
table_name = get_input_param('table_name')
target_folder = get_input_param('target_folder')

# COMMAND ----------

# SET VARIABLES
file_name = get_file_name(table_name, 'par')
source_file = get_file_path(source_folder, file_name)
target_file = get_file_path(target_folder, file_name)

print(source_file)
print(target_file)

# COMMAND ----------

# READ
main = spark.read.format('parquet').load(source_file)

# COMMAND ----------

# TRANSFORM
main_f = (
  main
  .transform(fix_cell_values())
  .transform(overwrite_value(['password']))
  .transform(remove_int_user_duplicates)
  .na.drop("all")
  .distinct()
)

main_f.cache()
main_f.display()

# COMMAND ----------

# VALIDATE
check_distinct_count(main_f, key_columns)

# COMMAND ----------

# SAVE
main_f.write.format('parquet').mode('overwrite').save(target_file)

# COMMAND ----------


