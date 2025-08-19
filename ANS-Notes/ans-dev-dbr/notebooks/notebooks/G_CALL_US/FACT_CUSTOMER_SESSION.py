# Databricks notebook source
# MAGIC %run ../_SHARED/FUNCTIONS/1.3/bootstrap

# COMMAND ----------

# INPUT
database_name = get_input_param('database_name', default_value = 'g_call_us')
incremental = get_input_param('incremental', 'bool', default_value = False)
key_columns = get_input_param('key_columns', 'list', default_value = ['CUSTOMER_SESSION_ID'])
overwrite = get_input_param('overwrite', 'bool', default_value = False)
prune_days = get_input_param('prune_days', 'int', default_value = 30)
sampling = get_input_param('sampling', 'bool', default_value = False)
table_name = get_input_param('table_name', default_value = 'fact_customer_session')
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
    f.AGENT_WRAPUP_DURATION,
    f.CAD_SURVEY_QUESTION_1,
    f.CAD_SURVEY_QUESTION_2
  FROM fact_customer_session_df f
  LEFT JOIN s_callsystems.DIM_QUEUE q ON f.QUEUE_ID = q.QUEUE_ID
  LEFT JOIN s_callsystems.DIM_ENTRYPOINT e ON f.ENTRYPOINT_ID = e.ENTRYPOINT_ID
  LEFT JOIN s_callsystems.DIM_TEAM t ON f.TEAM_ID = t.TEAM_ID
  WHERE 
  f._SOURCE = 'CTI'
  AND f._DELETED IS FALSE
  AND q.QUEUE_NAME IN (
    'OQ_NA_CS_COWANSVILLE_GENERAL',
    'OQ_NA_CS_COWANSVILLE_PARTNERS',
    'OQ_NA_CS_ISELIN',
    'OQ_NA_CS_RENO',			
    'OQ_NA_LAC_CS_BRAZIL',
    'OQ_NA_LAC_CS_MEXICO',
    'Q_GENERAL_DEFAULT',
    'Q_NA_CS_COWANSVILLE_DEFAULT',
    'Q_NA_CS_COWANSVILLE_GENERAL_ENGLISH',
    'Q_NA_CS_COWANSVILLE_GENERAL_FRENCH',
    'Q_NA_CS_COWANSVILLE_PARTNERS_ENGLISH',
    'Q_NA_CS_COWANSVILLE_PARTNERS_FRENCH',
    'Q_NA_CS_ISELIN_DEFAULT',
    'Q_NA_CS_ISELIN_POD1',
    'Q_NA_CS_ISELIN_POD2',
    'Q_NA_CS_ISELIN_POD3',
    'Q_NA_CS_ISELIN_POD4',
    'Q_NA_CS_MEXICO_DEFAULT',
    'Q_NA_CS_RENO',
    'Q_NA_CS_RENO_DEFAULT',
    'Q_NA_CS_RENO_POD1',
    'Q_NA_CS_RENO_POD2',
    'Q_NA_CS_RENO_POD3',
    'Q_NA_CS_RENO_POD4',
    'Q_NA_ONE_SOLUTION',
    'Q_NA-CANADA',
    'Q_NA-LAC_CS_BRAZIL_DEFAULT',
    'Q_SFAGENTID_BRAZIL',
    'Q_SFAGENTID_COWANSVILLE',
    'Q_SFAGENTID_COWANSVILLE_GEN_ENGLISH',
    'Q_SFAGENTID_COWANSVILLE_GEN_FRENCH',
    'Q_SFAGENTID_COWANSVILLE_PART_ENGLISH',
    'Q_SFAGENTID_COWANSVILLE_PART_FRENCH',
    'Q_SFAGENTID_ISELIN',
    'Q_SFAGENTID_MEXICO',
    'Q_SFAGENTID_RENO',
    'Q_USA_CUSTOMER_SERVICE',
    'Q_VM_NA_CS_COWANSVILLE_GENERAL',
    'Q_VM_NA_CS_COWANSVILLE_PARTNERS',
    'Q_VM_NA_CS_ISELIN',
    'Q_VM_NA_CS_RENO',
    'Q_NA_CS_ISELIN_POD4',
    'Q_NA_CS_RENO_POD4',
    'Q_NA_CS_ECOMMERCE',
    'Q_NA_CS_SUPPORT',
    'Unknown',
    'Q_NA_LAC_CS_BRAZIL',
    'Q_NA_CS_Mexico',
    'EP_NA_CS_Brazil',
    'EP_NA_CS_Mexico',
    'EP_NA_CS_Cowansville_General_Eng',
    'EP_NA_CS_Cowansville_General_Fr',
    'EP_NA_CS_Cowansville_General_LangMenu',
    'EP_NA_CS_Cowansville_Partners_Eng',
    'EP_NA_CS_Cowansville_Partners_Fr',
    'EP_NA_CS_Cowansville_Partners_LangMenu',
    'Q_NA_CS_Team1',
    'Q_NA_CS_Team10',
    'Q_NA_CS_Team11',
    'Q_NA_CS_Team12',
    'Q_NA_CS_Team13',
    'Q_NA_CS_Team14',
    'Q_NA_CS_Team15',
    'Q_NA_CS_Team16',
    'Q_NA_CS_Team17',
    'Q_NA_CS_Team18',
    'Q_NA_CS_Team19',
    'Q_NA_CS_Team2',
    'Q_NA_CS_Team20',
    'Q_NA_CS_Team21',
    'Q_NA_CS_Team22',
    'Q_NA_CS_Team23',
    'Q_NA_CS_Team24',
    'Q_NA_CS_Team25',
    'Q_NA_CS_Team3',
    'Q_NA_CS_Team4',
    'Q_NA_CS_Team5',
    'Q_NA_CS_Team6',
    'Q_NA_CS_Team7',
    'Q_NA_CS_Team8',
    'Q_NA_CS_Team9',
    'Q_NA_CS_SURVEYFLOW',
    'Q_NA_CS_POD1',
    'Q_NA_CS_POD2',
    'Q_NA_CS_POD3',
    'Q_NA_CS_POD4',
    'Q_NA_CS_POD5',
    'Q_NA_CS_POD_DEFAULT',
    'Q_Canada_SurveyEnd',
    'Q_Test_PostSurvey',
    'Q_PostSurvey_Canada',
    'Q_VM_NA_LAC_CS_Brazil',
    'Q_NA_CS_POD6',
    'Q_PostSurvey_Canada_French',
    'Q_SF_TEST',
    'Q_VM_NA_CS_AfterHours'   
  )
  AND e.ENTRYPOINT_NAME IN (
    'EP_NA_CS_Brazil',
    'EP_NA_CS_Cowansville_General_Eng',
    'EP_NA_CS_Cowansville_General_Fr',
    'EP_NA_CS_Cowansville_General_LangMenu',
    'EP_NA_CS_Cowansville_Partners_Eng',
    'EP_NA_CS_Cowansville_Partners_Fr',
    'EP_NA_CS_Cowansville_Partners_LangMenu',
    'EP_NA_CS_Iselin',
    'EP_NA_CS_Mexico',
    'EP_NA_CS_Reno',
    'EP_One_Solution',
    'OEP_LAC_Brazil',
    'OEP_NA_Cowansville_General',
    'OEP_NA_Cowansville_Partners',
    'OEP_NA_Iselin',
    'OEP_NA_Mexico',
    'OEP_NA_Reno',
    'Unknown',
    'EP_Survey_Prompt',
    'EP_Test_PostSurvey',
    'EP_Survey_Prompt_CAN_Partners',
    'EP_Survey_Prompt_CAN',
    'EP_PostSurvey_Canada_French',
    'EP_Survey_Prompt_CAN_French',
    'EP_PostSurvey_Canada',
    'EP_NA_CS_Survey'
  )
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
  AND f.SRC_PHONE_NUMBER NOT IN (
    '775-737-8412',
    '7757378412',
    '0017757378412',
    '17757378412',
    '450-931-0880',
    '4509310880',
    '0014509310880',
    '14509310880'
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


