# Databricks notebook source
# MAGIC %run ../_SHARED/FUNCTIONS/1.2/bootstrap

# COMMAND ----------

# MAGIC %run ../_SHARED/PARTIALS/1.2/header_s_core

# COMMAND ----------

# MAGIC %run ../_SCHEMA/s_core.customer_location

# COMMAND ----------

# LOAD DATASETS
if incremental:
  cutoff_value = get_cutoff_value(table_name, 'sapp01.vbpa', prune_days)
  main_inc = load_incr_dataset('sapp01.vbpa', '_MODIFIED', cutoff_value)
else:
  main_inc = load_full_dataset('sapp01.vbpa')


# COMMAND ----------

# SAMPLING
if sampling:
  main_inc = main_inc.limit(10)

# COMMAND ----------

# VIEWS
main_inc.createOrReplaceTempView('main_inc')

# COMMAND ----------

main = spark.sql("""
  SELECT distinct
    KNA1.ERNAM AS createdBy,
    CAST(KNA1.ERDAT AS TIMESTAMP) AS createdOn,
    CAST(NULL AS STRING) AS modifiedBy,
    CAST(NULL AS TIMESTAMP) AS modifiedOn,
    CURRENT_TIMESTAMP() AS insertedOn,
    CURRENT_TIMESTAMP() AS updatedOn, 
    KNA1.KUNNR AS accountId,
    KNA1.KUNNR AS accountNumber,
    CASE
       when vbpa.parvw ='RE' then Concat('BILL_TO-',kna1.kunnr)
       when vbpa.parvw ='WE' then Concat('SHIP_TO-',kna1.kunnr)
       when vbpa.parvw ='AG' then Concat('SOLD_TO-',kna1.kunnr)
       when vbpa.parvw ='RG' then Concat('PAY_TO-',kna1.kunnr)
    END  AS addressid, 
    KNA1.STRAS AS addressLine1,
    CAST(NULL AS STRING) AS addressLine2,
    CAST(NULL AS STRING) AS addressLine3,
    CAST(NULL AS STRING) AS addressLine4,
    KNA1.ORT01 AS city,
    KNA1.LAND1 AS country,
    KNA1.LAND1 AS countryCode,
    NULL AS county,
    CAST(NULL AS STRING) AS emailAddress,
    KNA1.KUKLA AS indiaIrpCustomerType,
    NULL AS mdmId,
    TRIM(Concat(Concat(Concat(TRIM(KNA1.NAME1),' ',TRIM(KNA1.NAME2)), ' ', TRIM(KNA1.NAME3)),' ',TRIM(KNA1.NAME4))) AS name,
    CAST(NULL AS STRING) AS partySiteNumber,
    ADRC.TEL_NUMBER AS phoneNumber,
    KNA1.PSTLZ AS postalCode,
    NULL AS primaryFlag,
    CAST(NULL AS STRING) AS siteCategory,
    CAST(NULL AS STRING) AS siteUseId,
    CASE
       when vbpa.parvw ='RE' then'BILL_TO'
       when vbpa.parvw ='WE' then'SHIP_TO'
       when vbpa.parvw ='AG' then'SOLD_TO'
       when vbpa.parvw ='RG' then'PAY_TO'
    END  AS siteUseCode, 
    NULL AS stateName,
    CAST(NULL AS STRING) AS state
  FROM main_inc vbpa
  LEFT JOIN sapp01.kna1 kna1 ON vbpa.kunnr = kna1.kunnr
  LEFT JOIN sapp01.ADRC adrc on  kna1.adrnr = adrc.ADDRNUMBER
  Where vbpa.parvw in ('RE','WE','AG','RG' )
""")

display(main)

# COMMAND ----------

columns = list(schema.keys())

# COMMAND ----------

main = remove_duplicate_rows(df = main, 
                             key_columns = ['addressId'], 
                             tableName = table_name, 
                             sourceName = source_name, 
                             notebookName = NOTEBOOK_NAME, 
                             notebookPath = NOTEBOOK_PATH)

# COMMAND ----------

# TRANSFORM DATA
main_f = (
  main
  .transform(tg_default(source_name))
  .transform(tg_core_customer_location())
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
  spark.sql("""
            SELECT 
                CASE
                   when vbpa.parvw ='RE' then Concat('BILL_TO-',kna1.kunnr)
                   when vbpa.parvw ='WE' then Concat('SHIP_TO-',kna1.kunnr)
                   when vbpa.parvw ='AG' then Concat('SOLD_TO-',kna1.kunnr)
                   when vbpa.parvw ='RG' then Concat('PAY_TO-',kna1.kunnr)
                END  AS addressid
             FROM sapp01.vbpa vbpa
            LEFT JOIN sapp01.kna1 kna1 ON vbpa.kunnr = kna1.kunnr
            LEFT JOIN sapp01.ADRC adrc on  kna1.adrnr = adrc.ADDRNUMBER
            Where vbpa.parvw in ('RE','WE','AG','RG' )
            """)
  .transform(attach_source_column(source = source_name))
  .transform(attach_primary_key('addressId,_SOURCE', 'edm.customer_location'))
  .select('_ID')
  .transform(add_unknown_ID())
)

apply_soft_delete(full_keys_f, table_name, key_columns = '_ID')

# COMMAND ----------

# UPDATE FK
update_foreign_key(table_name, 'createdBy,_SOURCE', 'createdBy_ID', 'edm.user')
update_foreign_key(table_name, 'modifiedBy,_SOURCE', 'modifiedBy_ID', 'edm.user')
update_foreign_key(table_name, 'accountId,_SOURCE', 'account_ID', 'edm.account')

# COMMAND ----------

# UPDATE CUTOFF VALUE
if not test_run:
  cutoff_value = get_incr_col_max_value(main_inc)
  update_cutoff_value(cutoff_value, table_name, 'sapp01.vbpa ')
  update_run_datetime(run_datetime, table_name, 'sapp01.vbpa ')

# COMMAND ----------

# MAGIC %run ../_SHARED/PARTIALS/1.2/footer_s_core
