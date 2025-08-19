# Databricks notebook source
# MAGIC %run ../../FUNCTIONS/1.1/bootstrap

# COMMAND ----------

# MAGIC %run ../../PARTIALS/1.1/header_bronze_notebook

# COMMAND ----------

# INPUT PARAMETERS
source_folder = get_param(params, "source_folder")
target_folder = get_param(params, "target_folder")
database_name = get_param(params, "database_name")
file_name = get_param(params, "file_name")
table_name = get_param(params, "table_name")
key_columns = get_param(params, "key_columns")
incremental = get_param(params, "incremental", "bool", False)
incremental_column = get_param(params, "incremental_column")
overwrite = get_param(params, "overwrite", "bool", False)
handle_delete = get_param(params, "handle_delete", "bool", False)
partition_column = get_param(params, "partition_column")
header = get_param(params, "header", "bool", True)
sampling = get_param(params, "sampling", "bool", False)
test_run = get_param(params, "test_run", "bool", False)
print_dict(params)

# COMMAND ----------

# VALIDATE INPUT
if source_folder is None or file_name is None:
  raise Exception("Input parameters are missing.")

if target_folder is None or database_name is None or table_name is None:
  raise Exception("Input parameters are missing.")
  
if incremental and key_columns is None:
  raise Exception("No join criteria for incremental")

# COMMAND ----------

# SETUP VARIABLES
source_file_path = DATALAKE_ENDPOINT + source_folder + '/' + file_name
target_file_path = DATALAKE_ENDPOINT + target_folder + '/' + table_name + '.par'
print(source_file_path)
print(target_file_path)

# COMMAND ----------

# READ DATA
main = (
  spark.read
  .format('csv')
  .option("header", header)
  .option("escape", "\\")
  .option("quote", "\"")
  .option("multiline", "true")
  .load(source_file_path)
)
main.display()

# COMMAND ----------

# SAMPLING
if sampling:
  if overwrite or key_columns is None:
    print('SKIPPING SAMPLING')
  else:
    print('SAMPLING ON')
    main = main.limit(10)

# COMMAND ----------

# READ DATA
main2 = (
  main
  .transform(fix_column_names)
  .transform(convert_nan_to_null)
  .transform(trim_all_values)
  .distinct()
)
display(main2)

# COMMAND ----------

# PARTITION DATA
if partition_column is not None:
  main3 = main2.transform(attach_partition_column(partition_column))
  partition_field = "_PART"
else:
  main3 = main2
  partition_field = None

# COMMAND ----------

# HANDLE DELETE
if handle_delete:
  main_f = main3.withColumn('_DELETED', f.lit(False))  
else:
  main_f = main3

# COMMAND ----------

# FULL OVERWRITE
if incremental == False and key_columns is None and sampling == False:
  register_hive_table(main_f, overwrite = True, partition_column = partition_field)
  append_into_hive_table(main_f)
  
# MERGE WITH KEYS
elif key_columns is not None:
  register_hive_table(main_f)
  merge_into_hive_table(main_f)

# SKIP MERGE WHEN NO JOIN CRITERIA DURING SAMPLING
elif sampling:
  print("SKIPPING MERGE")

# RAISE ERROR, NO JOIN CRITERIA  
else:
  raise Exception("NO JOIN CRITERIA")

# COMMAND ----------

# RETURN MAX VALUE
max_value = get_incr_col_max_value(main, incremental_column)
dbutils.notebook.exit(json.dumps({"max_value": max_value}))

# COMMAND ----------


