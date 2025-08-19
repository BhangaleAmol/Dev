# Databricks notebook source
# MAGIC %run ../_SHARED/FUNCTIONS/1.3/bootstrap 

# COMMAND ----------

# MAGIC %run ./_SHARED/func_callsystems

# COMMAND ----------

# INPUT PARAMETERS
database_name = get_input_param('database_name', default_value = 's_callsystems')
key_columns = get_input_param('key_columns', 'list', default_value = ['QUEUE_ID'])
overwrite = get_input_param('overwrite', 'bool', default_value = False)
sampling = get_input_param('sampling', 'bool', default_value = False)
table_name = get_input_param('table_name', default_value = 'dim_queue')
target_folder = get_input_param('target_folder', default_value = '/datalake_silver/callsystems/full_data')
temp_folder = get_input_param('temp_folder', default_value = '/datalake_silver/callsystems/temp_data')

# COMMAND ----------

# EXTRACT
file_path = get_file_path(temp_folder, 'clean_asrs.dlt')
asrs_df = spark.read.format('delta').load(file_path)

file_path = get_file_path(temp_folder, 'clean_csrs.dlt')
csrs_df = spark.read.format('delta').load(file_path)

# COMMAND ----------

# SAMPLING
if sampling:
  asrs_df = asrs_df.limit(10)
  csrs_df = csrs_df.limit(10)

# COMMAND ----------

# UNION
asrs_df2 = asrs_df.select('QUEUE_ID', 'QUEUE_SYSTEM_ID', 'QUEUE_NAME', 'CSTTS')
csrs_df2 = csrs_df.select('QUEUE_ID', 'QUEUE_SYSTEM_ID', 'QUEUE_NAME', 'CSTTS')
main_df = asrs_df2.union(csrs_df2)
main_df.createOrReplaceTempView('main_df')

# COMMAND ----------

# FILTER DATA
main_df2 = spark.sql("""
  SELECT QUEUE_ID, QUEUE_SYSTEM_ID, QUEUE_NAME, CSTTS
  FROM main_df
  WHERE 
    QUEUE_SYSTEM_ID IS NOT NULL 
    AND QUEUE_NAME IS NOT NULL
""")

main_df2.createOrReplaceTempView("main_df2")
main_df2.display()

# COMMAND ----------

# GET LATEST RECORDS
main_df3 = spark.sql("""
  SELECT 
    QUEUE_ID_NK,
    QUEUE_SYSTEM_ID_NK,
    QUEUE_NAME
  FROM (
    SELECT DISTINCT 
      QUEUE_ID AS QUEUE_ID_NK, 
      QUEUE_SYSTEM_ID AS QUEUE_SYSTEM_ID_NK,
      QUEUE_NAME,
      ROW_NUMBER() OVER (PARTITION BY QUEUE_SYSTEM_ID ORDER BY CSTTS DESC) AS NUM
    FROM main_df2    
  ) 
  WHERE NUM = 1
""")

main_df3.createOrReplaceTempView("main_df3")
main_df3.display()

# COMMAND ----------

main_df4 = (
  main_df3
  .transform(upper_queue_name)
  .transform(extract_region)
  .transform(extract_continent)
  .transform(extract_country)
  .transform(extract_city) 
  .transform(extract_location_static)
)

main_df4.display()

# COMMAND ----------

# ETL FIELDS
main_f = (
  main_df4
  .transform(attach_source_column('CTI'))
  .transform(attach_deleted_flag())
  .transform(attach_modified_date())
  .transform(attach_surrogate_key(['QUEUE_SYSTEM_ID_NK','_SOURCE'], name = 'QUEUE_ID'))
  .transform(attach_unknown_record)
)

main_f.display()

# COMMAND ----------

# VALIDATE
check_distinct_count(main_f, key_columns)

# COMMAND ----------

# LOAD
full_name = get_table_name(database_name, table_name)
register_hive_table(main_f, full_name, target_folder, options = {'overwrite': overwrite})
merge_into_table(main_f, full_name, key_columns)
