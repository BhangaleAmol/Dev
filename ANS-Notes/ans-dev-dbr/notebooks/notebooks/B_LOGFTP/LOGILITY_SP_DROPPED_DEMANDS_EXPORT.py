# Databricks notebook source
# MAGIC %run ../_SHARED/FUNCTIONS/1.3/bootstrap

# COMMAND ----------

# MAGIC %run ./_FUNCTIONS

# COMMAND ----------

# INPUT PARAMETERS
database_name = get_input_param('database_name')
file_name = get_input_param('file_name')
handle_delete = get_input_param('handle_delete', 'bool', False)
header = get_input_param('header', 'bool', True)
column_delimiter = get_input_param('column_delimiter','string','|')
incremental = get_input_param('incremental', 'bool', False)
incremental_column = get_input_param('incremental_column',default_value='_MODIFIED')
key_columns = get_input_param('key_columns')
overwrite = get_input_param('overwrite', 'bool', False)
partition_column = get_input_param('partition_column')
sampling = get_input_param('sampling', 'bool', False)
source_folder = get_input_param('source_folder')
table_name = get_input_param('table_name')
target_folder = get_input_param('target_folder')

# COMMAND ----------

# VALIDATE INPUT
if incremental and key_columns is None:
  raise Exception("INCREMENTAL & NO KEY COLUMNS")

if handle_delete and key_columns is None:
  raise Exception("HANDLE DELETE & NO KEY COLUMNS")
  
if overwrite and sampling:
  raise Exception("OVERWRITE & SAMPLING")

# COMMAND ----------

from datetime import datetime

# SETUP VARIABLES
src_file_name = get_latest_file_name(str(source_folder), str(file_name))

timestamp = re.findall(r'\d{14}', src_file_name)[0]
file_date = date(year=int(timestamp[0:4]), month=int(timestamp[4:6]), day=int(timestamp[6:8]))
print(file_date)

if not src_file_name:
  dbutils.notebook.exit('no source file available')

file_path = get_file_path(source_folder, src_file_name)

print('src_file_name: ' + src_file_name)
print('src_file_path: ' + file_path)

# COMMAND ----------

# READ DATA
main = (
  spark.read
  .format('csv')
  .option("header", "true")
  .option("escape", "\\")
  .option("quote", "\"")
  .option("delimiter",column_delimiter)
  .load(file_path)
)

# main.display()

# COMMAND ----------

# FIX COLUMNS
if 'Prop_0' in main.columns:

  header = main.filter(main['Prop_0'] == 'DMD_TYPE')
  headerColumn = header.first()
  main_2 = main.subtract(header)
  
  for column in main_2.columns:
    main_2 = main_2.withColumnRenamed(column, headerColumn[column])
else:
  main_2 = main

# COMMAND ----------

# SAMPLING
if sampling:
  main_2 = main_2.limit(10)

# COMMAND ----------

# BASIC CLEANUP
from pyspark.sql.functions import lit,col,current_date,last_day,next_day

# currenttime = datetime.date()

main_f = (
  main_2  
  .withColumn("generationDate", next_day(lit(file_date-timedelta(1)), "SAT"))
  .transform_if((partition_column is not None), attach_partition_column(partition_column))
  .transform(attach_modified_date())
  .transform(attach_deleted_flag())
  .distinct()
)

main_f.cache()
# main_f.display()
main_f.createOrReplaceTempView('main_f')
rowscount = main_f.count()
print(rowscount)

# COMMAND ----------

# VALIDATE DATA
if key_columns is not None:
  check_distinct_count(main_f, key_columns)

# COMMAND ----------

# LOAD
full_name = get_table_name(database_name, table_name)
register_hive_table(main_f, full_name, target_folder, options = {'overwrite': overwrite})
append_into_table(main_f, full_name, {"incremental_column":"GenerationDate"})

# COMMAND ----------

# RETURN MAX VALUE
if rowscount > 0:
  max_value = get_max_value(main_f, incremental_column)
else:
  print("No rows")
  # set it back to previous cutoff
  df = spark.table(full_name)
  max_value = get_max_value(df, incremental_column)
  
dbutils.notebook.exit(json.dumps({"max_value": max_value}))
