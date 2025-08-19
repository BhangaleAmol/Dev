# Databricks notebook source
# MAGIC %run ../_SHARED/FUNCTIONS/1.3/bootstrap

# COMMAND ----------

# MAGIC %run ../_SCHEMA/g_fin_qv.wc_pwbi_ap_invoice

# COMMAND ----------

# INPUT
database_name = get_input_param('database_name', default_value = 'g_fin_qv')
incremental = get_input_param('incremental', 'bool', default_value = False)
key_columns = get_input_param('key_columns', 'string', default_value = 'transactionDetailId, GL_JOURNAL_ID')
overwrite = get_input_param('overwrite', 'bool', default_value = False)
prune_days = get_input_param('prune_days', 'int', default_value = 30)
sampling = get_input_param('sampling', 'bool', default_value = False)
table_name = get_input_param('table_name', default_value = 'wc_pwbi_ap_invoice_a')
target_folder = get_input_param('target_folder', default_value = '/datalake_gold/fin_qv/full_data')

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
source_table = 's_finance.ap_transactions_ebs'
if incremental:
  cutoff_value = get_cutoff_value(target_table, source_table, prune_days)  
  ap_transactions_ebs = load_incremental_dataset(source_table, '_MODIFIED', cutoff_value)
else:
  ap_transactions_ebs = load_full_dataset(source_table)
  
ap_transactions_ebs.createOrReplaceTempView('ap_transactions_ebs')
# ap_transactions_ebs.display()

# COMMAND ----------

gl_linkage_lkp = spark.sql("""
SELECT DISTINCT
  INT(GLIMPREF.JE_HEADER_ID) || '-' || INT(GLIMPREF.JE_LINE_NUM) glJournalId,
  REPLACE(STRING(INT(T.LEDGER_ID)), ",", "") ledgerId,
  AELINE.AE_HEADER_ID || '-' || AELINE.AE_LINE_NUM slaTrxId,
  DLINK.SOURCE_DISTRIBUTION_TYPE || CASE
    WHEN DLINK.SOURCE_DISTRIBUTION_TYPE = 'AP_INV_DIST' THEN (
      CASE
        WHEN AELINE.ACCOUNTING_CLASS_CODE = 'LIABILITY' THEN '-LIABILITY'
        ELSE '-EXPENSE'
      END
    )
    ELSE (
      CASE
        WHEN DLINK.SOURCE_DISTRIBUTION_TYPE = 'AP_PMT_DIST' THEN '-' || AELINE.ACCOUNTING_CLASS_CODE
      END
    )
  END || '-' || INT(DLINK.SOURCE_DISTRIBUTION_ID_NUM_1)  AS sourceDistributionId
FROM
  ebs.GL_LEDGERS T,
  ebs.GL_PERIODS PER,
  ebs.GL_JE_HEADERS JHEADER,
  ebs.GL_IMPORT_REFERENCES GLIMPREF,
  ebs.XLA_AE_LINES AELINE,
  ebs.XLA_DISTRIBUTION_LINKS DLINK,
  ebs.GL_JE_BATCHES JBATCH
WHERE
  AELINE.GL_SL_LINK_TABLE = GLIMPREF.GL_SL_LINK_TABLE
  AND AELINE.GL_SL_LINK_ID = GLIMPREF.GL_SL_LINK_ID
  AND AELINE.AE_HEADER_ID = DLINK.AE_HEADER_ID
  AND AELINE.AE_LINE_NUM = DLINK.AE_LINE_NUM
  AND GLIMPREF.JE_HEADER_ID = JHEADER.JE_HEADER_ID
  AND JHEADER.JE_BATCH_ID = JBATCH.JE_BATCH_ID
  AND JHEADER.LEDGER_ID = T.LEDGER_ID
  AND JHEADER.STATUS = 'P'
  AND T.PERIOD_SET_NAME = PER.PERIOD_SET_NAME
  AND JHEADER.PERIOD_NAME = PER.PERIOD_NAME
  AND DECODE ('N', 'Y', T.LEDGER_ID, 1) IN (1) 
  AND DECODE ('N', 'Y', T.LEDGER_CATEGORY_CODE, 'NONE') IN ('NONE')
  AND DLINK.SOURCE_DISTRIBUTION_TYPE IN ('AP_INV_DIST')
  AND DLINK.APPLICATION_ID = 200
  AND AELINE.APPLICATION_ID = 200
""")
gl_linkage_lkp.createOrReplaceTempView('gl_linkage_lkp')

# COMMAND ----------

main = spark.sql("""
SELECT
ap.purchaseinvoicenumber as INVOICE_NUM,
ap.invoiceId as INVOICE_ID,
ap.owningBusinessUnitId as ORG_ID,
date_format(ap.postdate,'MMM-yy') as PERIOD_NAME,
to_Date(ap.postDate) as INV_GL_DATE,
sup.supplierNumber as VENDOR_NUM,
sup.supplierName as VENDOR_NAME,
null INVOICE_LINE_NUMBER,
ap.purchaseInvoiceLine LINE_NUMBER,
SUM(ap.documentAmount*-1 ) as INV_DIST_AMT,
ap.reversalFlag as AP_ADJ,
modified.login as INV_LAST_UPDATED_BY,
created.login as INV_CREATED_BY,
ap.dateInvoiced as INVOICE_DATE,
ap.currency as INVOICE_CURRENCY_CODE,
'DISTRIBUTION' as INVOICE_TYPE,
NULL as TAX_CODE,
ap.description as DESCRIPTION,
ap.invoiceHeaderAmount INVOICE_HEADER_AMT,
ap.totalTaxAmount as TOTAL_TAX_AMOUNT,
null as PAYMENT_DUE_DATE,
ap.paymentTermId as TERM,
'OPEN' POSTING_STATUS,
company.name COMPANY_NAME,
SUM(-1*ap.localAmount) AP_LOC_AMT,
SUM(-1*(ap.documentAmount / ap.exchangeRateUSD)) as AP_DOC_AMT_WITH_EXCG,
ap.currency as LOC_CURR_CODE,
gl_linkage.glJournalId as GL_JOURNAL_ID,
'POSTED' STATUS,
null as INVOICE_CLEARED_DATE,
ledger.name as LEDGER_NAME,
date_format(ap.postdate,'yyyy') as POSTED_YEAR,
referenceDocumentNumber as REF_DOC_NUM,
null as CLEARING_DOC_NUM,
'PAYABLES' as XACT_CODE,
case when ap.documentTypeId like '%INVDIST%' then 'INVDIST~ITEM' when ap.documentTypeId like '%STANDARD%' THEN 'STANDARD' END AS XACT_TYPE_CODE,
NULL AS VOID_PAYMENT,
current_date as W_INSERT_DT,
ap.transactionDetailId
FROM
ap_transactions_ebs ap 
join (
       select * from gl_linkage_lkp
 ) gl_linkage on gl_linkage.sourcedistributionid = ap.accountdocid --and gl_linkage.ledgerid = ap.ledgerid
left join s_core.supplier_account_ebs sup on ap.supplier_id = sup._id
left join s_core.ledger_ebs ledger on ap.ledger_id = ledger._id
left join s_core.user_ebs created on created._id = ap.createdby_id
left join s_core.user_ebs modified on modified._id = ap.modifiedBy_ID
left join s_core.organization_ebs company on company._id = ap.company_id and company.organizationType ='COMPANY'

 left join s_core.date on date_format(postdate,'yyyyMMdd') = date.dayid
where
ap._deleted = 'false'
--AND purchaseinvoicenumber = 'K2-0015/2021'
and date.fiscalYearId >= date_format(add_months(current_date, 6), 'yyyy')-2
GROUP BY
ap.purchaseinvoicenumber,
ap.invoiceId,
ap.owningBusinessUnitId ,
date_format(ap.postdate,'MMM-yy') ,
ap.postDate ,
sup.supplierNumber,
sup.supplierName,
ap.purchaseInvoiceLine,
modified.login,
created.login,
ap.dateInvoiced,
ap.currency ,
ap.description,
ap.invoiceHeaderAmount ,
ap.totalTaxAmount ,
ap.paymentTermId,
company.name ,
ap.currency,
gl_linkage.glJournalId ,
ledger.name,
date_format(ap.postdate,'yyyy'),
ap.referenceDocumentNumber,
case when ap.documentTypeId like '%INVDIST%' then 'INVDIST~ITEM' when ap.documentTypeId like '%STANDARD%' THEN 'STANDARD' END,
ap.reversalFlag,
ap.transactionDetailId
--ORDER BY gl_linkage.glJournalId
""")

# COMMAND ----------

# TRANSFORM DATA
main_f = (
   main
   .transform(attach_partition_column("INV_GL_DATE"))
   .transform(attach_modified_date())
   .transform(attach_deleted_flag())
   .transform(apply_schema(schema))
)

if incremental:
  main_f.display()

# COMMAND ----------

# LOAD
options = {'overwrite': overwrite, 'partition_column': '_PART'}
register_hive_table(main_f, target_table, target_folder, options = options)
merge_into_table(main_f, target_table, key_columns, options = {'auto_merge': True})

# COMMAND ----------

# UPDATE CUTOFF VALUE
if not sampling:
  cutoff_value = get_max_value(main_f, '_MODIFIED')
  update_cutoff_value(cutoff_value, target_table, 's_finance.ap_transactions_ebs')
  update_run_datetime(run_datetime, target_table, 's_finance.ap_transactions_ebs')
