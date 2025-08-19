# Databricks notebook source
# MAGIC %run ../_SHARED/FUNCTIONS/1.3/bootstrap

# COMMAND ----------

# INPUT
database_name = get_input_param('database_name', default_value = 'g_call_emea')
incremental = get_input_param('incremental', 'bool', default_value = False)
key_columns = get_input_param('key_columns', 'list', default_value = ['CUSTOMER_SESSION_ID'])
overwrite = get_input_param('overwrite', 'bool', default_value = False)
prune_days = get_input_param('prune_days', 'int', default_value = 30)
sampling = get_input_param('sampling', 'bool', default_value = False)
table_name = get_input_param('table_name', default_value = 'fact_customer_session')
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
source_table = 's_callsystems.fact_customer_session'
if incremental:
  cutoff_value = get_cutoff_value(target_table, source_table, prune_days)
  fact_customer_session_df = load_incremental_dataset(source_table, '_MODIFIED', cutoff_value)
else:
  fact_customer_session_df = load_full_dataset(source_table)

fact_customer_session_df.createOrReplaceTempView('fact_customer_session_df')
fact_customer_session_df.display()

# COMMAND ----------

# SAMPLING
if sampling:
  fact_customer_session_df = fact_customer_session_df.limit(10)
  fact_customer_session_df.createOrReplaceTempView('fact_customer_session_df')

# COMMAND ----------

fact_customer_session_f = spark.sql("""
  SELECT 
    f.CUSTOMER_SESSION_ID,
    f.AGENT_ID,
    f.ENTRYPOINT_ID,
    f.QUEUE_ID,
    f.SITE_ID,
    f.TEAM_ID,
    f.COMPANY_ID,
    f.DATE_ID,
    f.CALL_DIRECTION,
    f.CETTS,
    f.CSTTS,
    f.SRC_PHONE_NUMBER,
    f.DST_PHONE_NUMBER,
    f.HANDLED,
    f.TERMINATING_END,
    f.TERMINATION_TYPE,
    f.CONFERENCE_COUNT,
    f.CONFERENCE_DURATION,
    f.CTQ_COUNT,
    f.CTQ_DURATION,
    f.DELETED_RECORD,
    f.DURATION,
    f.HOLD_COUNT,
    f.HOLD_DURATION,
    f.IVR_COUNT,
    f.IVR_DURATION,
    f.QUEUE_COUNT,
    f.QUEUE_DURATION,
    f.TALK_COUNT,
    f.TALK_DURATION,
    f.TERMINATION_COUNT,
    f.WRAPUP_DURATION,
    f.WRAPUP_CODE_NAME,
    f.AGENT_CONFERENCE_DURATION,
    f.AGENT_CONSULT_ANSWER_DURATION,
    f.AGENT_CONSULT_REQUEST_DURATION,
    f.AGENT_NOTRESPONDING_DURATION,
    f.AGENT_HOLD_DURATION,
    f.AGENT_RINGING_DURATION,
    f.AGENT_TALK_DURATION,
    f.AGENT_WRAPUP_DURATION
  FROM fact_customer_session_df f
  LEFT JOIN s_callsystems.DIM_QUEUE q ON f.QUEUE_ID = q.QUEUE_ID
  LEFT JOIN s_callsystems.DIM_ENTRYPOINT e ON f.ENTRYPOINT_ID = e.ENTRYPOINT_ID
  LEFT JOIN s_callsystems.DIM_TEAM t ON f.TEAM_ID = t.TEAM_ID
  WHERE 
  f._SOURCE = 'CTI'
  AND f._DELETED IS FALSE
  AND q.QUEUE_NAME IN (
    'Q_APAC_CS_JAPAN',
	'Q_APAC_CS_AUSTRALIA',
	'Q_APAC_CS_KOREA',
	'Q_EMEA_CS_EUFRMED',
	'Q_EMEA_CS_EUDEVMARK_REC',
	'Q_EMEA_CS_MM_France_REC',
	'Q_EMEA_CS_EUKEYACC_REC',
	'Q_SFAgentId_REC',
	'Q_EMEA_CS_MM_UK_IRE_REC',
	'Q_EMEA_CS_MM_Northern Europe_REC',
	'Q_EMEA_CS_APS',
	'Q_EMEA_CS_GERMED_Rec',
	'Q_EMEA_CS_MM_Spain Portugal_REC',
	'Q_EMEA_CS_APS_Rec',
	'Q_EMEA_CS_MM_France',
	'Q_EMEA_CS_EUKEYACC',
	'Q_EMEA_CS_MATUREMARKET',
	'Q_EMEA_CS_GERMIND_REC',
	'Q_EMEA_CS_GERMEDIND',
	'Q_EMEA_CS_MM_Central Europe_Rec',
	'Q_SFAgentId_Australia',
	'Q_EMEA_CS_EUDEVMARK',
	'Q_EMEA_CS_MM_Northern Europe',
	'Q_VM_EMEA_MATUREMARKET',
	'Q_APAC_CS_ROA',
	'Q_EMEA_CS_MM_Central Europe',
	'Q_SFAgentId_Korea',
	'Q_EMEA_CS_Beatrice_Riffaut_Rec',
	'Q_EMEA_CS_MM_Spain Portugal',
	'Q_EMEA_CS_MM_UK_IRE',
	'Q_EMEA_CS_Daniel_Lozano_Bischoff_REC',
	'Q_EMEA_IS_Yves_Ramensperger_REC',
	'Q_General_Default_Rec',
	'Q_APAC_CS_CHINA',
	'Q_APAC_CS_Jade_Carroll',
	'Q_EMEA_CS_Anna Wawrzyniak',
	'Q_SFAgentId',
	'Q_SFAgentId_Cergy_REC',
	'Q_SFAgentId_ROA',
	'Q_NA_CS_Cowansville_General_French',
	'Q_NA_CS_Reno_POD4',
	'Q_General_Default',
	'Unknown'
  
  )
  AND e.ENTRYPOINT_NAME IN (
    'EP_APAC_CS_JAPAN',
	'EP_APAC_CS_AUSTRALIA',
	'EP_APAC_CS_KOREA',
	'EP_EMEA_CS_EUFRMED',
	'EP_EMEA_CS_EUDEVMARK_Main',
	'EP_EMEA_CS_MM_France_Main',
	'EP_EMEA_CS_EUKEYACC_Main',
	'EP_EMEA_CS_MM_Northern Europe_Main',
	'EP_EMEA_CS_MM_UK_IRE_Main',
	'EP_EMEA_CS_GERMED_Main',
	'EP_EMEA_CS_MM_Spain Portugal_Main',
	'EP_EMEA_CS_APS_Main',
	'EP_EMEA_CS_GERMIND_Main',
	'EP_APAC_CS_Jade Carroll',
	'EP_EMEA_CS_Daniel Lozano Bischoff_Main',
	'EP_EMEA_CS_MM_Central Europe_Main',
	'EP_APAC_CS_Shana_Evers',
	'EP_NA_CS_Iselin',
	'EP_APAC_CS_ROA',
	'EP_EMEA_CS_Beatrice_Riffaut',
	'EP_APAC_CS_Joanna Hart',
	'EP_APAC_CS_CHINA',
	'EP_EMEA_CS_MM_Spain Portugal_REC',
	'EP_EMEA_CS_EUDEVMARK',
	'EP_EMEA_CS_MM_UK_IRE_REC',
	'EP_EMEA_CS_Daniel Lozano Bischoff_REC',
	'EP_APAC_CS_Elizabeth_Jones',
	'EP_EMEA_CS_Anna Wawrzyniak_Main',
	'EP_EMEA_CS_EUKEYACC_REC',
	'EP_EMEA_CS_GERMED_REC',
	'EP_EMEA_CS_MM_France_REC',
	'EP_NA_CS_Cowansville_General_LangMenu',
	'Unknown'
  )
  AND t.TEAM_NAME IN (
    'Team_APAC_CS_AUSTRALIA',
	'VM_APAC_CS_Australia',
	'Team_APAC_CS_CHINA',
	'VM_APAC_CS_CHINA',
	'Team_APAC_CS_Jade_Carroll',
	'Team_JAPAN-1',
	'Team_JAPAN-2',
	'VM_APAC_CS_JAPAN',
	'Team_ROA',
	'VM_APAC_ROA',
	'Team_EMEA_CS_MM_France',
	'EP_EMEA_CS_GERMIND',
	'EMEA_CS_APS',
	'VM_EMEA_CS_MATUREMARKET',
	'Team_EMEA_CS_MM_Span_Port',
	'EMEA_CS_EUDEVMARK',
	'VM_EMEA_CS_APS',
	'EMEA_IS_Yves_Ramensperger',
	'Team_EUKEYACC',
	'Team_EMEA_CS_MM_UK_IRE',
	'EMEA_IS_Emmanuel_Mollura',
	'Team_GERMEDIND',
	'EP_EMEA_CS_GERMED',
	'Team_EMEA_CS_MM_NE',
	'Team_MATUREMARKET',
	'EMEA_IS_Jason_Braham',
	'Team_EMEA_CS_MM_Italy',
	'VM_EMEA_IS_Emmanuel_Mollura',
	'VM_EMEA_CS_GERMEDIND',
	'VM_EMEA_IS_Jason_Braham',
	'Team_EUFRMED',
	'Z_Team_Test_NA',
	'Team_ROA',
	'EMEA_IS_Philippe_de_Haer',
	'VM_EMEA_IS_Philippe_de_Haer',
	'EMEA_IS_Alexandra_Ley',
	'VM_EMEA_CS_EUFRMED',
	'VM_EMEA_IS_Yves_Ramensperger',
	'VM_EMEA_CS_EUDEVMARK',
	'EMEA_CS_Marie Antoinette Navarro',
	'Team_JAPAN-1',
	'Unknown'
  )
""")

fact_customer_session_f.display()

# COMMAND ----------

# LOAD
register_hive_table(fact_customer_session_f, target_table, target_folder, options = {'overwrite': overwrite})
merge_into_table(fact_customer_session_f, target_table, key_columns)

# COMMAND ----------

# UPDATE CUTOFF VALUE
if not sampling:
  cutoff_value = get_max_value(fact_customer_session_df, '_MODIFIED')
  update_cutoff_value(cutoff_value, target_table, 's_callsystems.fact_customer_session')
  update_run_datetime(run_datetime, target_table, 's_callsystems.fact_customer_session')

# COMMAND ----------


