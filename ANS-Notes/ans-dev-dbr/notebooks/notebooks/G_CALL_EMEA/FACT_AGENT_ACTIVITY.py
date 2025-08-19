# Databricks notebook source
# MAGIC %run ../_SHARED/FUNCTIONS/1.3/bootstrap

# COMMAND ----------

# INPUT
database_name = get_input_param('database_name', default_value = 'g_call_emea')
incremental = get_input_param('incremental', 'bool', default_value = False)
key_columns = get_input_param('key_columns', 'list', default_value = ['ACTIVITY_ID'])
overwrite = get_input_param('overwrite', 'bool', default_value = False)
prune_days = get_input_param('prune_days', 'int', default_value = 30)
sampling = get_input_param('sampling', 'bool', default_value = False)
table_name = get_input_param('table_name', default_value = 'fact_agent_activity')
target_folder = get_input_param('target_folder', default_value = '/datalake_gold/g_call_emea/full_data')

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
source_table = 's_callsystems.fact_agent_activity'
if incremental:
  cutoff_value = get_cutoff_value(target_table, source_table, prune_days)
  fact_agent_activity_df = load_incremental_dataset(source_table, '_MODIFIED', cutoff_value)
else:
  fact_agent_activity_df = load_full_dataset(source_table)

fact_agent_activity_df.createOrReplaceTempView('fact_agent_activity_df')
fact_agent_activity_df.display()

# COMMAND ----------

# SAMPLING
if sampling:
  fact_agent_activity_df = fact_agent_activity_df.limit(10)
  fact_agent_activity_df.createOrReplaceTempView('fact_agent_activity_df')

# COMMAND ----------

fact_agent_activity_f = spark.sql("""
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
    f.DURATION,
    f.IDLE_CODE_NAME,
    f.WRAPUP_CODE_NAME    
  FROM fact_agent_activity_df f
  LEFT JOIN s_callsystems.dim_team t ON f.TEAM_ID = t.TEAM_ID
  WHERE 
    f._SOURCE = 'CTI'
    AND f._DELETED IS FALSE
    AND t.TEAM_NAME IN (
    		'Team_JAPAN-1',
			'Team_APAC_CS_AUSTRALIA',
			'Team_KOREA',
			'VM_EMEA_CS_EUFRMED',
			'EMEA_CS_EUDEVMARK',
			'Team_EMEA_CS_MM_UK_IRE',
			'Team_EUKEYACC',
			'Team_EUFRMED',
			'Team_EMEA_CS_MM_NE',
			'EP_EMEA_CS_GERMED',
			'Team_EMEA_CS_MM_Italy',
			'Team_EMEA_CS_MM_Span_Port',
			'EMEA_CS_APS',
			'EP_EMEA_CS_GERMIND',
			'Team_EMEA_CS_MM_France',
			'VM_EMEA_EUKEYACC',
			'VM_APAC_CS_Australia',
			'VM_EMEA_CS_APS',
			'VM_EMEA_CS_MATUREMARKET',
			'Team_ROA',
			'VM_APAC_CS_JAPAN',
			'VM_EMEA_CS_GERMEDIND',
			'VM_APAC_ROA',
			'EMEA_IS_Yves_Ramensperger',
			'VM_APAC_CS_CHINA',
			'VM_EMEA_IS_Yves_Ramensperger',
			'Team_APAC_CS_CHINA',
			'VM_EMEA_CS_EUDEVMARK',
			'Team_Korea2',
			'VM_APAC_US_KOREA',
			'Unknown'
  )
""")

fact_agent_activity_f.display()

# COMMAND ----------

# LOAD
register_hive_table(fact_agent_activity_f, target_table, target_folder, options = {'overwrite': overwrite})
merge_into_table(fact_agent_activity_f, target_table, key_columns)

# COMMAND ----------

# UPDATE CUTOFF VALUE
if not sampling:
  cutoff_value = get_max_value(fact_agent_activity_df, '_MODIFIED')
  update_cutoff_value(cutoff_value, target_table, 's_callsystems.fact_agent_activity')
  update_run_datetime(run_datetime, target_table, 's_callsystems.fact_agent_activity')

# COMMAND ----------


