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
incremental = get_input_param('incremental', 'bool', False)
incremental_column = get_input_param('incremental_column','string','_MODIFIED')
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

# database_name = "logftp" # [str]
# file_name = "LOGILITY_DO.DAILY_Level_1_for_MPX*.CSV" # [str]
# handle_delete = False # [bool]
# header = True # [bool]
# incremental = False # [bool]
# incremental_column = None # [NoneType]
# key_columns = "Lvl1Fcst,Lvl2Fcst,Lvl3Fcst" # [str]
# overwrite = True # [bool]
# partition_column = None # [NoneType]
# sampling = False # [bool]
# source_folder = "/datalake/LOGFTP/raw_data/delta_data" # [str]
# table_name = "LOGILITY_DO_LEVEL_1_FOR_MPX" # [str]
# target_folder = "/datalake/LOGFTP/raw_data/full_data" # [str]

# COMMAND ----------

# SETUP VARIABLES
src_file_name = get_latest_file_name(source_folder, file_name)

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
  .option("multiline", "true")
  .load(file_path)
)
main.display()

# COMMAND ----------

if 'Prop_0' in main.columns:

  header = main.filter(main['Prop_0'] == 'FCST LVL')
  headerColumn = header.first()
  main_2 = main.subtract(header)
  
  for column in main_2.columns:
    main_2 = main_2.withColumnRenamed(column, headerColumn[column])
else:
  main_2 = main
  
for column in main_2.columns:
  main_2 = main_2.withColumnRenamed(column, column.replace(' ', ''))

column_list = main_2.columns
column_list[1] = 'Lvl1Fcst'
column_list[2] = 'Lvl2Fcst'
column_list[3] = 'Lvl3Fcst'
column_list[6] = 'Field_03'
column_list[7] = 'Field_09'
column_list[8] = 'Field_10'
column_list[9] = 'Field_12'
column_list[10] = 'Field_13'
column_list[11] = 'Field_47'
main_3 = main_2.toDF(*column_list)
main_3.display()

# COMMAND ----------

# SAMPLING
if sampling:
  main_3 = main_3.limit(10)

# COMMAND ----------

# BASIC CLEANUP
main_f = (
  main_3  
  .transform_if((partition_column is not None), attach_partition_column(partition_column))
  .transform(attach_modified_date())
  .transform(attach_deleted_flag())
  .distinct()
)

main_f.cache()
main_f.display()

# COMMAND ----------

# VALIDATE DATA
if key_columns is not None:
  check_distinct_count(main_f, key_columns)

# COMMAND ----------

# LOAD
full_name = get_table_name(database_name, table_name)
register_hive_table(main_f, full_name, target_folder, options = {'overwrite': overwrite})
merge_into_table(main_f, full_name, key_columns, options = {'auto_merge': True})

# COMMAND ----------

# RETURN MAX VALUE
max_value = get_max_value(main_f, incremental_column)
dbutils.notebook.exit(json.dumps({"max_value": max_value}))
