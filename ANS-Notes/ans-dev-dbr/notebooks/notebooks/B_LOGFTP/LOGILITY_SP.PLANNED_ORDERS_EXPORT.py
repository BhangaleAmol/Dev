# Databricks notebook source
# MAGIC %run ../_SHARED/FUNCTIONS/1.3/bootstrap

# COMMAND ----------

# MAGIC %run ./_FUNCTIONS

# COMMAND ----------

# DBTITLE 1,Get Param values
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

def read_data_file(source_file, schema):
  return (spark.read
    .format('csv')
    .option("header", True)
    .option("escape", "\"")
    .option("quote", "\"")
    .option("multiline", "true")
    .option("delimiter",column_delimiter)
    .schema(schema)
    .load(source_file)
    .distinct()        
  )

target_table = get_table_name(database_name,table_name,None)
print(target_table,file_path)

schema = StructType([
    StructField("Item",StringType(), True),
    StructField("Location_DC", StringType(), True),
    StructField("Source_Location", StringType(), True),    
    StructField("Production_Resources", StringType(), True),   
    StructField("Ship_Date", StringType(), True),
    StructField("Quantity", StringType(), True),
    StructField("UOM", StringType(), True),
    StructField("Planner_ID", StringType(), True),
    StructField("Avg_Dep_Dmd", StringType(), True),
    StructField("Safety_Stock_Quantity", StringType(), True)    
])

main = read_data_file(file_path, schema)
main.createOrReplaceTempView('POOrders')
# main.display() 
main.count() 

# COMMAND ----------

# SAMPLING
if sampling:
  main = main.limit(10)

# COMMAND ----------

from pyspark.sql.functions import next_day,last_day,lit,date_format,to_date,regexp_replace,col

# BASIC CLEANUP
main_f = (
  main
  .withColumn("GenerationDate",next_day(lit(file_date-timedelta(1)), "SAT")) # weekly snapshots on Sat
  .withColumn("Ship_Date",date_format(to_date(regexp_replace(col("Ship_Date"),":000000000",""),"yyyyMMdd"),"yyyy-MM-dd").cast("date"))
  .transform_if((partition_column is not None), attach_partition_column(partition_column))
  .transform(attach_modified_date())
  .transform(attach_deleted_flag())
  .distinct()
)

main_f.cache()
# main_f.display()
rowscount = main_f.count()
print(rowscount)

# COMMAND ----------

# VALIDATE DATA
if key_columns is not None:
  check_distinct_count(main_f, key_columns)

# COMMAND ----------

# LOAD
register_hive_table(main_f, target_table, target_folder, options = {'overwrite': overwrite})
append_into_table(main_f, target_table, {"incremental_column":"GenerationDate"})

# COMMAND ----------

# DBTITLE 1,Purge monthly generations after retaining 12 months
sqlq=f"DELETE FROM {target_table} WHERE GenerationDate <= add_months(last_day(current_date),-13)"
spark.sql(sqlq)

# COMMAND ----------

# RETURN MAX VALUE
if rowscount > 0:
  max_value = get_max_value(main_f, incremental_column)
else:
  print("No rows")
  # set it back to previous cutoff
  df = spark.table(target_table)
  max_value = get_max_value(df, incremental_column)

# update_cutoff_value(max_value, target_table, None)
dbutils.notebook.exit(json.dumps({"max_value": max_value}))
