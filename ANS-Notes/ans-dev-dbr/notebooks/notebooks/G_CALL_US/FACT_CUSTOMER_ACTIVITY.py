# Databricks notebook source
# MAGIC %run ../_SHARED/FUNCTIONS/1.3/bootstrap

# COMMAND ----------

# INPUT
database_name = get_input_param('database_name', default_value = 'g_call_us')
incremental = get_input_param('incremental', 'bool', default_value = False)
key_columns = get_input_param('key_columns', 'list', default_value = ['ACTIVITY_ID'])
overwrite = get_input_param('overwrite', 'bool', default_value = False)
prune_days = get_input_param('prune_days', 'int', default_value = 30)
sampling = get_input_param('sampling', 'bool', default_value = False)
table_name = get_input_param('table_name', default_value = 'fact_customer_activity')
target_folder = get_input_param('target_folder', default_value = '/datalake_gold/g_call_us/full_data')

# COMMAND ----------

# VALIDATE INPUT
if incremental and key_columns is None:
  raise Exception("INCREMENTAL & NO KEY COLUMNS")

if overwrite and sampling:
  raise Exception("OVERWRITE & SAMPLING")

# COMMAND ----------

# VARIABLES
target_table = get_table_name(database_name, table_name)

# COMMAND ----------

# EXTRACT
source_table = 's_callsystems.fact_customer_activity'
if incremental:
  cutoff_value = get_cutoff_value(target_table, source_table, prune_days)
  fact_customer_activity_df = load_incremental_dataset(source_table, '_MODIFIED', cutoff_value)
else:
  fact_customer_activity_df = load_full_dataset(source_table)

fact_customer_activity_df.createOrReplaceTempView('fact_customer_activity_df')
fact_customer_activity_df.display()

# COMMAND ----------

# SAMPLING
if sampling:
  fact_customer_activity_df = fact_customer_activity_df.limit(10)
  fact_customer_activity_df.createOrReplaceTempView('fact_customer_activity_df')

# COMMAND ----------

fact_customer_activity_f = spark.sql("""
  SELECT 
    f.AGENT_SESSION_ID,
    f.CUSTOMER_SESSION_ID,
    f.DATE_ID,
    f.ACTIVITY_ID,    
    f.AGENT_ID,
    f.QUEUE_ID,
    f.SITE_ID,
    f.TEAM_ID,
    f.CETTS,
    f.CSTTS,
    f.TYPE,
    f.DURATION 
  FROM fact_customer_activity_df f
  LEFT JOIN s_callsystems.dim_team t ON f.TEAM_ID = t.TEAM_ID
  WHERE
    f.CURRENT_STATE = 'CONNECT'
    AND f._SOURCE = 'CTI'
    AND f._DELETED IS FALSE
    AND t.TEAM_NAME IN (
      'NA_CS_Cowansville_General',
      'NA_CS_Cowansville_Partners',
      'NA_CS_Iselin_POD1',
      'NA_CS_Iselin_POD2',
      'NA_CS_Iselin_POD3',
      'NA_CS_Iselin_POD4',
      'NA_CS_Mexico',
      'NA_CS_Reno_POD1',
      'NA_CS_Reno_POD2',
      'NA_CS_Reno_POD3',
      'NA_CS_Reno_POD4',	
      'NA_CS_Reno_POD1',
      'NA_CS_Reno_POD2',
      'NA_CS_Reno_POD3',
      'NA_CS_Reno_POD4',
      'NA_CS_Support',
      'NA_CS_Ecommerce',
      'NA_LAC_CS_Brazil',
      'NA_One_Solution',			
      'Unknown',
      'VM_NA_Cowansville_General',
      'VM_NA_Cowensville_Partners',
      'VM_NA_CS_Brazil',
      'VM_NA_CS_Iselin',
      'VM_NA_CS_Mexico',
      'VM_NA_CS_One_Solution',
      'VM_NA_CS_Reno',
      'VM_NA_CA_CS',
      'NA_CS_Team1',
      'NA_CS_Team10',
      'NA_CS_Team11',
      'NA_CS_Team12',
      'NA_CS_Team13',
      'NA_CS_Team14',
      'NA_CS_Team15',
      'NA_CS_Team16',
      'NA_CS_Team17',
      'NA_CS_Team18',
      'NA_CS_Team19',
      'NA_CS_Team2',
      'NA_CS_Team20',
      'NA_CS_Team21',
      'NA_CS_Team22',
      'NA_CS_Team23',
      'NA_CS_Team24',
      'NA_CS_Team25',
      'NA_CS_Team3',
      'NA_CS_Team4',
      'NA_CS_Team5',
      'NA_CS_Team6',
      'NA_CS_Team7',
      'NA_CS_Team8',
      'NA_CS_Team9',
      'NA_CS_SURVEYFLOW',
      'NA_CS_POD1',
      'NA_CS_POD2',
      'NA_CS_POD3',
      'NA_CS_POD4',
      'NA_CS_POD5',
      'NA_CS_POD_DEFAULT'
  )
""")

fact_customer_activity_f.display()

# COMMAND ----------

# LOAD
register_hive_table(fact_customer_activity_f, target_table, target_folder, options = {'overwrite': overwrite})
merge_into_table(fact_customer_activity_f, target_table, key_columns)

# COMMAND ----------

# UPDATE CUTOFF VALUE
if not sampling:
  cutoff_value = get_max_value(fact_customer_activity_df, '_MODIFIED')
  update_cutoff_value(cutoff_value, target_table, 's_callsystems.fact_customer_activity')
  update_run_datetime(run_datetime, target_table, 's_callsystems.fact_customer_activity')
