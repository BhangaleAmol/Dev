# Databricks notebook source
# MAGIC %run ../_SHARED/FUNCTIONS/1.2/bootstrap

# COMMAND ----------

# MAGIC %run ../_SHARED/PARTIALS/1.2/header_s_core

# COMMAND ----------

# MAGIC %run ../_SCHEMA/s_core.account_organization

# COMMAND ----------

# LOAD DATASETS
if incremental:
  cutoff_value = get_cutoff_value(table_name, 'sapp01.knvv', prune_days)
  sap_knvv_inc = load_incr_dataset('sapp01.knvv', '_MODIFIED', cutoff_value)
else:
  sap_knvv_inc = load_full_dataset('sapp01.knvv')

# COMMAND ----------

# SAMPLING
if sampling:
  sap_knvv_inc = sap_knvv_inc.limit(10)

# COMMAND ----------

# VIEWS
sap_knvv_inc.createOrReplaceTempView('sap_knvv_inc')

# COMMAND ----------

KNVP_LKP = spark.sql(
"""Select * from (
  select 
    mandt, 
    kunnr, 
    vkorg, 
    vtweg, 
    spart, 
    parza, 
    kunn2,
    ROW_NUMBER() OVER(PARTITION BY mandt,kunnr,vkorg,vtweg,spart order by mandt,kunnr,vkorg,vtweg,spart ) RowNum
from sapp01.knvp    
) 
b where RowNum=1""")
KNVP_LKP.createOrReplaceTempView('KNVP_LKP_TBL')
KNVP_LKP.display()

# COMMAND ----------

csr_lkp = spark.sql("""
SELECT DISTINCT knvv.kunnr, knvplkp.MANDT, knvplkp.VKORG, knvplkp.VTWEG, knvplkp.SPART, kna1.name1 csr
FROM sap_knvv_inc knvv
LEFT JOIN KNVP_LKP_TBL knvplkp
  ON  knvplkp.MANDT = knvv.MANDT
  AND knvplkp.KUNNR = knvv.KUNNR
  AND knvplkp.VKORG = knvv.VKORG
  AND knvplkp.VTWEG = knvv.VTWEG
  AND knvplkp.SPART = knvv.SPART
LEFT JOIN sapp01.kna1 
  on kna1.mandt = knvplkp.mandt
  and kna1.kunnr = knvplkp.kunn2
""")
csr_lkp.createOrReplaceTempView('csr_lkp')

# COMMAND ----------

main = spark.sql("""
                  select
                    KNVV.ERNAM createdBy,
                    TO_TIMESTAMP(KNVV.ERDAT) AS createdOn,
                    NULL AS modifiedBy,
                    CAST(NULL AS TIMESTAMP) AS modifiedOn,
                    CURRENT_TIMESTAMP() AS insertedOn,
                    CURRENT_TIMESTAMP() AS updatedOn,
                    KNVV.KUNNR accountId,
                    csr_lkp.csr,
                    KNVV.WAERS currency,
                    KNVV.SPART customerDivision,
                    KNVV.VTWEG distributionChannel,
--                     KNVV.ZCUSTOM forecastGroup,
                    KNVV.CUSTGROUP AS forecastGroup,
                    CASE
                       LEFT(bzirk,1) WHEN 'I' THEN 'INDUSTRIAL'
                       WHEN 'M' THEN 'MEDICAL'
                       ELSE 'OTHER'
                    END AS gbu,
                    KNVV.VKGRP salesGroup,
                    KNVV.VKBUR salesOffice,
                    KNVV.VKORG salesOrganization
                  from
                    sap_knvv_inc knvv
                    LEFT join csr_lkp 
                      ON  csr_lkp.MANDT = knvv.MANDT
                      AND csr_lkp.kunnr = knvv.kunnr
                      AND csr_lkp.VKORG = knvv.VKORG
                      AND csr_lkp.VTWEG = knvv.VTWEG
                      AND csr_lkp.SPART = knvv.SPART
                    
""")

main.cache()
print(main.count())
main.display()

# COMMAND ----------

main.createOrReplaceTempView('main')

# COMMAND ----------

columns = list(schema.keys())

# COMMAND ----------

main = remove_duplicate_rows(df = main, 
                             key_columns = ['accountId','customerDivision', 'salesOrganization', 'distributionChannel'], 
                             tableName = table_name, 
                             sourceName = source_name, 
                             notebookName = NOTEBOOK_NAME, 
                             notebookPath = NOTEBOOK_PATH)

# COMMAND ----------

# TRANSFORM DATA
main_f = (
  main
  .transform(tg_default(source_name))
  .transform(tg_core_account_organization())
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
      KUNNR AS accountId,
      SPART AS customerDivision, 
      VKORG AS salesOrganization, 
      VTWEG AS distributionChannel
  FROM sapp01.knvv
  """)
  .transform(attach_source_column(source = source_name))
  .transform(attach_primary_key('accountId, customerDivision, salesOrganization, distributionChannel,_SOURCE','edm.account_organization'))
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
  cutoff_value = get_incr_col_max_value(sap_knvv_inc, '_MODIFIED')
  update_cutoff_value(cutoff_value, table_name, 'sapp01.knvv')
  update_run_datetime(run_datetime, table_name, 'sapp01.knvv')

# COMMAND ----------

# MAGIC %run ../_SHARED/PARTIALS/1.2/footer_s_core
