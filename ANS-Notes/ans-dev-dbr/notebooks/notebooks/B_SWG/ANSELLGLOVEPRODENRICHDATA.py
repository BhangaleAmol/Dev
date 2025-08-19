# Databricks notebook source
# MAGIC %run ../_SHARED/FUNCTIONS/1.2/bootstrap

# COMMAND ----------

# MAGIC %run ../_SHARED/PARTIALS/1.2/header_b_swg

# COMMAND ----------

# MAGIC %run ./_FUNCTIONS

# COMMAND ----------

# VALIDATE
if source_folder is None or table_name is None:
  raise Exception("source data details are missing")

if target_folder is None or database_name is None or table_name is None:
  raise Exception("target details are missing")

if incremental and overwrite:
  raise Exception("incremental & overwrite not allowed")

# COMMAND ----------

# DEBUG
# source_folder = '/datalake/SWG/raw_data/delta_data'
# database_name = 'swg'
# table_name = 'ansellgloveprodenrichdata'
# target_folder = '/datalake/SWG/raw_data/full_data'
# partition_columns = ['dt','batch']

# COMMAND ----------

# VARIABLES
file_name = get_file_name(None, table_name)
table_name = get_table_name(database_name, None, table_name)
source_file_path = get_file_path(source_folder, f'{file_name}.par', target_container, target_storage)
source_file_bad_data_path = get_file_path(f'{source_folder}_bad' , f'{file_name}.par', target_container, target_storage)
target_file_path = get_file_path(target_folder, f'{file_name}.par', target_container, target_storage)

print(f'table_name: {table_name}')
print(f'file_name: {file_name}')
print(f'source_file_path: {source_file_path}')
print(f'source_file_bad_data_path: {source_file_bad_data_path}')
print(f'target_file_path: {target_file_path}')

# COMMAND ----------

# DEFINE CORRECT SCHEMA
from pyspark.sql.types import *

schema = StructType([
    StructField('batch',StringType(),True),
    StructField('batchId',StringType(),True),
	StructField('deviceId',StringType(),True),
    StructField('dt',DateType(),True),
    StructField('durationSecs',LongType(),True),
	StructField('durationSecs1',LongType(),True),
	StructField('fle_ext_count_slow',LongType(),True),
	StructField('fle_ext_count_fast',LongType(),True),
    StructField('fle_ext_dur',LongType(),True),
    StructField('hand',StringType(),True),
    StructField('haptic_violation_count',LongType(),True),
    StructField('idle_dur',LongType(),True),
    StructField('jobFunctionId',StringType(),True),
    StructField('localTImeStampStart',StringType(),True),
	StructField('localTImeStampEnd',StringType(),True),
    StructField('locationId',StringType(),True),
    StructField('organizationId',StringType(),True),
    StructField('payloadCountFinal',LongType(),True),
    StructField('payloadCountInit',LongType(),True),
	StructField('rad_uln_count_slow',LongType(),True),
	StructField('rad_uln_count_fast',LongType(),True),
    StructField('rad_uln_dur',LongType(),True),
    StructField('rollAngle',DoubleType(),True),
    StructField('runId',StringType(),True),
	StructField('sup_pro_count_slow',LongType(),True),
	StructField('sup_pro_count_fast',LongType(),True),	
	StructField('sup_pro_dur',LongType(),True),
    StructField('thumbsUpHaptic',DoubleType(),True), # new field
    StructField('thumbsUpHaptic_count',LongType(),True), # new field
    StructField('thumbsUpThreshold',DoubleType(),True),
    StructField('thumbsUpVibration',DoubleType(),True),
    StructField('thumbsUpWindowSize',DoubleType(),True),
    StructField('timedHaptic',DoubleType(),True), # new field
    StructField('timedHaptic_count',LongType(),True), # new field
    StructField('timedHapticWindowSize',DoubleType(),True), # new field
    StructField('timeStampEnd',StringType(),True),
    StructField('timeStampStart',StringType(),True),
	StructField('tot_dur',LongType(),True),
	StructField('walk_dur',LongType(),True),	
	StructField('wrist_score',DoubleType(),True),	
	StructField('userId',StringType(),True)	
])

# COMMAND ----------

if not path_exists(source_file_path):
  dbutils.notebook.exit("No new data")

# COMMAND ----------

# FILTER BAD DATA
file_paths = get_file_paths(source_file_path)
move_bad_data(schema, source_file_path, file_paths, source_file_bad_data_path)

# COMMAND ----------

spark.conf.set("spark.databricks.io.cache.enabled", "false")

# COMMAND ----------

# READ GOOD DATA
good_data_1 = (
  spark.read.format('parquet')
  .option("mergeSchema", "true")
  .option("basePath", source_file_path)
  .load(source_file_path)
)

if hive_table_exists(table_name):
  table_columns = spark.table(table_name).columns
  good_data_2 = good_data_1.transform(t_add_missing_columns(columns = table_columns))
else:
  good_data_2 = good_data_1

good_data_f = (
  good_data_2
  .transform(t_add_missing_columns(columns = [
    'thumbsUpHaptic', 
    'timedHapticWindowSize', 
    'thumbsUpHaptic_count',
    'timedHaptic',
    'timedHaptic_count',
    'timedHapticWindowSize'
  ]))
  .transform(t_unify_data_types())
  .transform(sort_columns)
)
  
good_data_f.count()

# COMMAND ----------

# READ BAD DATA
if path_exists(source_file_bad_data_path):
  bad_data_f = (
    spark.read.format('parquet')
    .option("mergeSchema", "true")
    .option("basePath", source_file_bad_data_path)
    .load(source_file_bad_data_path)
    .transform(t_add_missing_columns(columns = good_data_f.columns))
    .transform(t_unify_data_types())
    .transform(sort_columns)
  )
 
  good_data_f = (
    good_data_f
    .transform(t_add_missing_columns(columns = bad_data_f.columns))
    .transform(sort_columns)
  )

# COMMAND ----------

# UNION
if path_exists(source_file_bad_data_path):  
  main_1 = good_data_f.union(bad_data_f.select(good_data_f.columns))
else:  
  main_1 = good_data_f

# COMMAND ----------

# MAIN_F
main_f = (
  main_1
  .withColumn('dt', f.when(f.col('dt') == '2022-00-00', '2022-01-01').otherwise(f.col('dt')))
  .transform(t_cast_types())
  .withColumn('_DELETED', f.lit(False))
  .withColumn('_MODIFIED', f.current_timestamp()) 
  .transform(sort_columns)
)

main_f.display()

# COMMAND ----------

# PERSIST DATA
write_mode = 'overwrite' if overwrite else 'append'

(
  main_f.write
  .option('mergeSchema', 'true')
  .option('path', target_file_path)
  .partitionBy(partition_columns)
  .mode(write_mode)
  .saveAsTable(table_name)
)

# COMMAND ----------

# REMOVE BAD DATA
dbutils.fs.rm(source_file_bad_data_path, True)
