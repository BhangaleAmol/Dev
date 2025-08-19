# Databricks notebook source
# MAGIC %run ../_SHARED/FUNCTIONS/1.2/bootstrap

# COMMAND ----------

# MAGIC %run ../_SHARED/PARTIALS/1.2/header_s_supplychain

# COMMAND ----------

# MAGIC %run ../_SCHEMA/s_supplychain.buyer

# COMMAND ----------

# LOAD DATASETS
xx_per_all_people_f = spark.table('ebs.xx_per_all_people_f')
if sampling: xx_per_all_people_f = xx_per_all_people_f.limit(10)
xx_per_all_people_f.createOrReplaceTempView('xx_per_all_people_f')

po_agents = spark.table(' ebs.po_agents')
if sampling: po_agents = po_agents.limit(10)
po_agents.createOrReplaceTempView('po_agents')

# COMMAND ----------

TMP_lookup=spark.sql("""SELECT Max(EFFECTIVE_START_DATE) AS ABC,CAST(P.PERSON_ID AS INT) AS Agent_ID
,MAX(P.FULL_NAME) AS Buyer
FROM po_agents BUY 
LEFT JOIN 
xx_per_all_people_f P 
ON P.PERSON_ID = BUY.AGENT_ID
Group by P.Person_ID""")
TMP_lookup.createOrReplaceTempView("TMP_lookup")
TMP_lookup.cache()
TMP_lookup.count()

# COMMAND ----------

main = spark.sql("""
select DISTINCT 
       NULL AS createdBy
      ,CAST(NULL AS TIMESTAMP) AS createdOn
      ,NULL AS modifiedBy
      ,CAST(NULL AS TIMESTAMP) AS modifiedOn
      ,CAST(NULL AS TIMESTAMP) AS insertedOn
      ,CAST(NULL AS TIMESTAMP) AS updatedOn
      ,Agent_ID AS buyerId
      ,Buyer AS name 
  from TMP_lookup""")
display(main)

# COMMAND ----------

columns = list(schema.keys())

# COMMAND ----------

main = remove_duplicate_rows(df = main, 
                             key_columns = ['buyerId'], 
                             tableName = table_name, 
                             sourceName = source_name, 
                             notebookName = NOTEBOOK_NAME, 
                             notebookPath = NOTEBOOK_PATH)

# COMMAND ----------

# TRANSFORM DATA
main_f = (
  main  
  .transform(tg_default(source_name))
  .transform(tg_supplychain_buyer())
  .transform(apply_schema(schema))  
  .transform(attach_unknown_record)
  .select(columns)
  .transform(sort_columns)
)

main_f.cache()
display(main_f)

# COMMAND ----------

# PERSIST DATA
options = {'target_storage': target_storage, 'target_container': target_container}
merge_to_delta(main_f, table_name, target_folder, overwrite, options = options)

# COMMAND ----------

# HANDLE DELETE
full_keys_f = (
  spark.sql("""
            SELECT Max(EFFECTIVE_START_DATE) AS ABC,CAST(P.PERSON_ID AS INT) AS buyerId
            ,MAX(P.FULL_NAME) AS Buyer
            FROM po_agents BUY 
            LEFT JOIN 
            xx_per_all_people_f P 
            ON P.PERSON_ID = BUY.AGENT_ID
            Group by P.Person_ID
   """)
  .transform(attach_source_column(source = source_name))
  .transform(attach_surrogate_key(columns = 'buyerId,_SOURCE'))
  .select('_ID')
  .transform(add_unknown_ID())
)

apply_soft_delete(full_keys_f, table_name, key_columns = '_ID')

# COMMAND ----------

# UPDATE FK
update_foreign_key(table_name, 'createdBy,_SOURCE', 'createdBy_ID', 'edm.user')
update_foreign_key(table_name, 'modifiedBy,_SOURCE', 'modifiedBy_ID', 'edm.user')

# COMMAND ----------

# UPDATE CUTOFF VALUE
if not test_run:
  cutoff_value = get_incr_col_max_value(xx_per_all_people_f)
  update_cutoff_value(cutoff_value, table_name, 'ebs.xx_per_all_people_f')
  update_run_datetime(run_datetime, table_name, 'ebs.xx_per_all_people_f')
  
  cutoff_value = get_incr_col_max_value(po_agents)
  update_cutoff_value(cutoff_value, table_name, 'ebs.po_agents')
  update_run_datetime(run_datetime, table_name, 'ebs.po_agents')

# COMMAND ----------

# MAGIC %run ../_SHARED/PARTIALS/1.2/footer_s_supplychain
