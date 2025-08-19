# Databricks notebook source
# MAGIC %run ../_SHARED/FUNCTIONS/1.3/bootstrap 

# COMMAND ----------

# INPUT PARAMETERS
database_name = get_input_param('database_name', default_value = 's_callsystems')
key_columns = get_input_param('key_columns', 'list', default_value = ['ENTRYPOINT_ID'])
overwrite = get_input_param('overwrite', 'bool', default_value = False)
sampling = get_input_param('sampling', 'bool', default_value = False)
table_name = get_input_param('table_name', default_value = 'dim_entrypoint')
target_folder = get_input_param('target_folder', default_value = '/datalake_silver/callsystems/full_data')
temp_folder = get_input_param('temp_folder', default_value = '/datalake_silver/callsystems/temp_data')

# COMMAND ----------

# EXTRACT
file_path = get_file_path(temp_folder, 'clean_csrs.dlt')
csrs_df = spark.read.format('delta').load(file_path)
csrs_df.createOrReplaceTempView("csrs_df")

# COMMAND ----------

# SAMPLING
if sampling:
  csrs_df = csrs_df.limit(10)
  csrs_df.createOrReplaceTempView('csrs_df')

# COMMAND ----------

# FILTER RECORDS
main_df = spark.sql("""
  SELECT DISTINCT 
    ENTRYPOINT_ID, 
    ENTRYPOINT_SYSTEM_ID, 
    ENTRYPOINT_NAME,
    CAST(CSTTS AS TIMESTAMP)
  FROM csrs_df
  WHERE 
    ENTRYPOINT_SYSTEM_ID IS NOT NULL
    AND ENTRYPOINT_NAME IS NOT NULL
""")

main_df.createOrReplaceTempView('main_df')

# COMMAND ----------

# GET LASTEST RECORDS
main_df2 = spark.sql("""
  SELECT 
    ENTRYPOINT_ID_NK,
    ENTRYPOINT_SYSTEM_ID_NK,
    ENTRYPOINT_NAME
  FROM (
    SELECT DISTINCT 
      ENTRYPOINT_ID AS ENTRYPOINT_ID_NK, 
      ENTRYPOINT_SYSTEM_ID AS ENTRYPOINT_SYSTEM_ID_NK,
      ENTRYPOINT_NAME,
      ROW_NUMBER() OVER (PARTITION BY ENTRYPOINT_SYSTEM_ID ORDER BY CSTTS DESC) AS NUM
    FROM main_df    
  ) 
  WHERE NUM = 1
""")

# COMMAND ----------

# ETL FIELDS
main_f = (
  main_df2
  .transform(attach_source_column('CTI'))
  .transform(attach_deleted_flag())
  .transform(attach_modified_date())
  .transform(attach_surrogate_key(['ENTRYPOINT_SYSTEM_ID_NK','_SOURCE'], name = 'ENTRYPOINT_ID'))
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
