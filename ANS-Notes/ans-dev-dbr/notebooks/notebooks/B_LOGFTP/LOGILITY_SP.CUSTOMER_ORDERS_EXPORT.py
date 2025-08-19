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
incremental_column = get_input_param('incremental_column',default_value="_MODIFIED")
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
    StructField("Location", StringType(), True),
    StructField("Customer", StringType(), True),      
    StructField("CO_Order_Number", StringType(), True),
    StructField("CO_Line_Number", IntegerType(), True),
    StructField("CO_Requested_Date", StringType(), True),
    StructField("CO_Fulfilment_Date", StringType(), True),
    StructField("OrderQuantity", StringType(), True),    
    StructField("CO_Planned_Qty", StringType(), True)        
])

# columns = ['Item','Location','Customer','QV','OrderNumber','LineNumber','RequestedDate','FulfilmentDate','OrderQuantity','RequestedQty']
DSOrders = read_data_file(file_path, schema)
DSOrders.createOrReplaceTempView('DSOrders')
DSOrders.display() #1565406

# COMMAND ----------

# SAMPLING
if sampling:
  DSOrders = DSOrders.limit(10)

# COMMAND ----------

# BASIC CLEANUP
main_f = (
  DSOrders
  .transform_if((partition_column is not None), attach_partition_column(partition_column))
  .transform(attach_modified_date())
  .transform(attach_deleted_flag())
  .distinct()
)

main_f.cache()
main_f.display()
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
merge_into_table(main_f, full_name, key_columns, options = {'auto_merge': True})

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
