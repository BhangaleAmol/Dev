# Databricks notebook source
# MAGIC %run ../_SHARED/FUNCTIONS/1.2/bootstrap

# COMMAND ----------

# MAGIC %run ../_SHARED/PARTIALS/1.2/header_s_service

# COMMAND ----------

# MAGIC %run ../_SCHEMA/s_service.quality_creditnote_approval

# COMMAND ----------

# LOAD DATASETS
if incremental:
  cutoff_value = get_cutoff_value(table_name, 'com.qara_dashboard', prune_days)
  main_inc = load_incr_dataset('com.qara_dashboard', '_modified', cutoff_value)
else:
  main_inc = load_full_dataset('com.qara_dashboard')


# COMMAND ----------

spark.conf.set("spark.sql.legacy.timeParserPolicy", "CORRECTED")

# COMMAND ----------

# SAMPLING
if sampling:
  main_inc = main_inc.limit(10)

# COMMAND ----------

# VIEWS
main_inc.createOrReplaceTempView('main_inc')

# COMMAND ----------

# MAGIC %sql
# MAGIC desc smartsheets.edm_control_table 

# COMMAND ----------

reporting_lookup = spark.sql("""
SELECT DISTINCT 
   KEY_VALUE,nvl(CVALUE,'') CVALUE, nvl(NVALUE,'') NVALUE
FROM smartsheets.edm_control_table 
WHERE TABLE_ID = 'COMIND_COMPLAINTS_VALUES' 
  and ACTIVE_FLG = true 
  and current_date between VALID_FROM and VALID_TO
  """)
reporting_lookup.createOrReplaceTempView('reporting_lookup')

# COMMAND ----------

main = spark.sql("""
SELECT
      c.creatoremail AS createdBy
     ,TO_TIMESTAMP(c.created, "yyyy-MM-dd'T'HH:mm:ss")  AS createdOn
     ,NULL  AS modifiedBy -- LastModifiedById
     ,c._Modified  AS modifiedOn 
     ,current_timestamp  AS insertedOn
     ,current_timestamp AS updatedOn
     ,c.EXPApprover1 AS approver1
     ,c.EXPApprover1date AS approver1Date
     ,c.EXPApprover2 AS approver2
     ,c.EXPApprover2date AS approver2Date
     ,c.EXPApprover3 AS approver3
     ,c.EXPApprover3date AS approver3Date
     ,c.EXPApprover4 AS approver4
     ,c.EXPApprover4date AS approver4Date
     ,c.EXPApprover5 AS approver5
     ,c.EXPApprover5date AS approver5Date
     ,c.Amount AS complaintAmount
     ,Case when c.Currency = 'EUR' then c.Amount  Else (c.Amount/usd_exch.exchangeRate)*usd_exch1.exchangeRate      END AS complaintAmountEur
     ,Case when c.Currency = 'USD' then c.Amount  Else c.Amount/usd_exch.exchangeRate      END AS  complaintAmountUsd
     ,c.Descriptionofcomplaint AS complaintDescription
     ,c.ID AS complaintId
     ,c.Status AS complaintStatus
     ,c.Title AS complaintTitle
     ,c.Complainttype AS complaintType
     ,c.OtherComplainttype AS complaintType2
     ,c.Country AS country
     ,c.DateofCNQA AS creditNoteDate
     ,c.CNnumber AS creditNoteNumber
     ,c.EXPCNValue AS creditNoteValue
     ,c.CSR AS csr
     ,c.Currency AS currency
     ,c.CustomerDistributorName AS distributorName
     ,c.GBU AS gbu
     ,c.Involved AS involved
     ,c.EXPLotnbr AS lotNumber
     ,c.Factory AS originName
     ,c.EXPProductinformation AS productName
     ,c.EXPQuantity AS quantity
     ,c.Region AS region
     ,rl.KEY_VALUE AS reportingGroup
     ,c.RequestType AS requestType
     ,c.OtherRequestType AS requestType2
     ,c.TrackwiseDigitalNbr AS trackWiseDigitalNumber
     ,c.EXPTrackwisenbr AS trackwiseNumber
     ,c.EXPUnitofmeasure AS unitOfMeasure
FROM
  main_inc c
   LEFT JOIN s_core.exchange_rate_agg usd_exch on usd_exch.fromcurrency = c.currency
      and  date_format(c.DateofCNQA, 'yyyyMM') BETWEEN date_format(usd_exch.startdate,'yyyyMM') AND date_format(usd_exch.enddate,'yyyyMM') 
      and usd_exch.ratetype = 'BUDGET'
  LEFT JOIN s_core.exchange_rate_agg usd_exch1 on usd_exch1.fromcurrency = 'EUR'  
      and  date_format(c.DateofCNQA, 'yyyyMM') BETWEEN date_format(usd_exch1.startdate,'yyyyMM') AND date_format(usd_exch1.enddate,'yyyyMM') 
      and usd_exch1.ratetype = 'BUDGET'
  Left Join reporting_lookup rl on CAST((Case when c.Currency = 'USD' then c.Amount  Else c.Amount/usd_exch.exchangeRate      END) AS DECIMAL(22,7))  > rl.CVALUE and CAST((Case when c.Currency = 'USD' then c.Amount  Else c.Amount/usd_exch.exchangeRate      END) AS DECIMAL(22,7))< rl.NVALUE

""")

main.cache()
display(main)

# COMMAND ----------

columns = list(schema.keys())

# COMMAND ----------

main = remove_duplicate_rows(df = main, 
                             key_columns = ['complaintId'], 
                             tableName = table_name, 
                             sourceName = source_name, 
                             notebookName = NOTEBOOK_NAME, 
                             notebookPath = NOTEBOOK_PATH)

# COMMAND ----------

# TRANSFORM DATA
main_f = (
  main
  .transform(tg_default(source_name))
  .transform(tg_service_quality_creditnote_approval())
  .transform(apply_schema(schema))  
  .transform(attach_unknown_record)
  .select(columns)
  .transform(sort_columns)
)

main_f.cache()
main_f.display()

# COMMAND ----------

# PERSIST DATA
options = {'target_storage': target_storage, 'target_container': target_container}
merge_to_delta(main_f, table_name, target_folder, overwrite, options = options)

# COMMAND ----------

# HANDLE DELETE

full_keys_f = (
  spark.table('com.qara_dashboard')
  .filter('_DELETED IS FALSE')
  .selectExpr('ID AS complaintId')
  .transform(attach_source_column(source = source_name))
  .transform(attach_surrogate_key(columns = 'complaintId,_SOURCE'))
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
  cutoff_value = get_incr_col_max_value(main_inc, "_MODIFIED")
  update_cutoff_value(cutoff_value, table_name, 'com.qara_dashboard')
  update_run_datetime(run_datetime, table_name, 'com.qara_dashboard')

# COMMAND ----------

# MAGIC %run ../_SHARED/PARTIALS/1.2/footer_s_service
