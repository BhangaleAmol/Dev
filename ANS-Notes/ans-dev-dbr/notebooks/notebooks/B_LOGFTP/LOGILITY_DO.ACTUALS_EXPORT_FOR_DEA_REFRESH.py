# Databricks notebook source
# MAGIC %run ../_SHARED/FUNCTIONS/1.3/bootstrap

# COMMAND ----------

# MAGIC %run ./_FUNCTIONS

# COMMAND ----------

# INPUT PARAMETERS
database_name = get_input_param('database_name')
file_name = get_input_param('file_name')
handle_delete = get_input_param('handle_delete', 'bool', False)
column_delimiter = get_input_param('column_delimiter','string',',')
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

# SETUP VARIABLES
src_file_name = get_latest_file_name(source_folder, file_name)

if not src_file_name:
  dbutils.notebook.exit('no source file available')

file_path = get_file_path(source_folder, src_file_name)
full_name = get_table_name(database_name, table_name)

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
  .option("multiline", "true")
  .load(file_path)
)
# main.display()
rowscount = main.count()

# COMMAND ----------

# checking if the file is already loaded then dont load it again and set back the max to the previous value
if not overwrite:
  sql=f"select (select if(count(*)>0, true, false) from {full_name} where _SOURCEFILENAME = '{src_file_name}') as isFileLoaded,(select nvl(date_format(max({incremental_column}), 'yyyy-MM-dd HH:mm:ss'),'1900-01-01') from {full_name}) as maxDate"
  df = spark.sql(sql)
  isFileLoaded = df.collect()[0][0]
  maxDate = df.collect()[0][1]
  print(rowscount,isFileLoaded,maxDate)
  
  if isFileLoaded or rowscount==0:
    dbutils.notebook.exit(json.dumps({"max_value": maxDate}))

# COMMAND ----------

# FIX HEADER
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
column_list[2] = 'Lvl1Fcst'
column_list[3] = 'Lvl2Fcst'
column_list[4] = 'Lvl3Fcst'
main_3 = main_2.toDF(*column_list)

# main_3.display()

# COMMAND ----------

# get ASOFYR & ASOFMN

main_4 = (
  main_3
  .filter('ASOFYR IS NOT NULL') 
  .filter('ASOFMN IS NOT NULL')
)

column_row = main_4.select('ASOFYR', 'ASOFMN').collect()[1]
year_value = column_row['ASOFYR']
month_value = column_row['ASOFMN']

print(f'ASOFYR: {year_value}, ASOFMN: {month_value}')

# COMMAND ----------

# RENAME PERIOD COLUMNS
from datetime import datetime
from dateutil.relativedelta import relativedelta
from pyspark.sql.functions import lit,col,current_timestamp

date = datetime(int(year_value), int(month_value), 1)
prev_date = date

for i in range(1, 25):
  future_date = date + relativedelta(months = i)
  
  for c in main_4.columns:
    if c == f'ACTD{prev_date.month}/{str(prev_date.year)[2:]}':
      main_4 = main_4.withColumnRenamed(c, f'ACTD{i}')
    if c == f'ADJD{prev_date.month}/{str(prev_date.year)[2:]}':
      main_4 = main_4.withColumnRenamed(c, f'ADJD{i}')
    if c == f'FUTO{future_date.month}/{str(future_date.year)[2:]}':
      main_4 = main_4.withColumnRenamed(c, f'FUTO{i}')
    if c == f'ADSD{prev_date.month}/{str(prev_date.year)[2:]}':
      main_4 = main_4.withColumnRenamed(c, f'ADSD{i}')
    if c == f'AD1F{prev_date.month}/{str(prev_date.year)[2:]}':
      main_4 = main_4.withColumnRenamed(c, f'AD1F{i}')
      
  prev_date = date - relativedelta(months = i) 
      
# main_4.display()

# COMMAND ----------

# SAMPLING
if sampling:
  main_4 = main_4.limit(10)

# COMMAND ----------

# BASIC CLEANUP
main_f = (
  main_4  
  .transform_if((partition_column is not None), attach_partition_column(partition_column))
  .transform(attach_modified_date())
  .transform(attach_deleted_flag())
  .withColumn("_SOURCEFILENAME",lit(src_file_name))
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
if rowscount > 0:
  full_name = get_table_name(database_name, table_name)
  register_hive_table(main_f, full_name, target_folder, options = {'overwrite': overwrite})
  merge_into_table(main_f, full_name, key_columns, options = {'auto_merge': True})

# COMMAND ----------

# RETURN MAX VALUE
if rowscount > 0:
  max_value = get_max_value(main_f, incremental_column)
else:
  print("No rows")
  # set it back to previous cutoff  
  max_value = maxDate
  
dbutils.notebook.exit(json.dumps({"max_value": max_value}))
