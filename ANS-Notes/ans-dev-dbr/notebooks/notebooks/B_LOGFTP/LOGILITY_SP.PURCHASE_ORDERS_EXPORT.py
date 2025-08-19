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

schema = StructType([
    StructField("Item",StringType(), True),
    StructField("Location_DC", StringType(), True),
    StructField("Source_Location", StringType(), True),   
    StructField("Need_By_Date", StringType(), True),
    StructField("Quantity", DecimalType(), True),
    StructField("UOM", StringType(), True),
    StructField("Planner_ID", StringType(), True),
    StructField("Order_ID", StringType(), True),
    StructField("Shipment_ID", StringType(), True),
    StructField("Logility_User_ID", StringType(), True)    
])

POOrders = read_data_file(file_path, schema)
POOrders.createOrReplaceTempView('POOrders')
# POOrders.display()
rowscount = POOrders.count()

# COMMAND ----------

# checking if the file is already loaded then dont load it again and set back the max to the previous value
sql=f"select (select if(count(*)>0, true, false) from {full_name} where _SOURCEFILENAME = '{src_file_name}') as isFileLoaded,(select nvl(date_format(max({incremental_column}), 'yyyy-MM-dd HH:mm:ss'),'1900-01-01') from {full_name}) as maxDate"
df = spark.sql(sql)
isFileLoaded = df.collect()[0][0]
maxDate = df.collect()[0][1]

print(rowscount,isFileLoaded,maxDate)
  
if isFileLoaded or rowscount==0:
  dbutils.notebook.exit(json.dumps({"max_value": maxDate}))

# COMMAND ----------

# DBTITLE 1,Remove Duplicate rows
from pyspark.sql.functions import lit,col,sum as _sum

Orig_cols = POOrders.columns
grouping_cols = [c for c in Orig_cols if c != 'Quantity']
POOrders = (POOrders
              .groupBy(grouping_cols).agg(_sum("Quantity").alias("Quantity"))
              .select(Orig_cols)
           )

# POOrders.display()

# COMMAND ----------

# SAMPLING
if sampling:
  POOrders = POOrders.limit(10)

# COMMAND ----------

# BASIC CLEANUP
main_f = (
  POOrders
  .transform_if((partition_column is not None), attach_partition_column(partition_column))
  .transform(attach_modified_date())
  .transform(attach_deleted_flag())
  .withColumn("_SOURCEFILENAME",lit(src_file_name))
  .distinct()
)

# main_f.display()

# COMMAND ----------

# VALIDATE DATA
if key_columns is not None:
  check_distinct_count(main_f, key_columns)

# COMMAND ----------

# LOAD
if rowscount > 0:
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
