# Databricks notebook source
# MAGIC %run ../_SHARED/FUNCTIONS/1.2/bootstrap

# COMMAND ----------

# MAGIC %run ../_SHARED/PARTIALS/1.2/header_silver_notebook

# COMMAND ----------

# MAGIC %run ../_SHARED/PARTIALS/1.2/header_s_supplychain

# COMMAND ----------

# MAGIC %run ../_SCHEMA/s_supplychain.purchase_order_headers

# COMMAND ----------

# LOAD DATASETS
if incremental:
  cutoff_value = get_cutoff_value(table_name, 'ebs.PO_HEADERS_ALL', prune_days)
  po_headers_all_inc = load_incr_dataset('ebs.PO_HEADERS_ALL', 'LAST_UPDATE_DATE', cutoff_value)
else:
  po_headers_all_inc = load_full_dataset('ebs.PO_HEADERS_ALL')

# COMMAND ----------

# SAMPLING
if sampling:
  po_headers_all_inc = po_headers_all_inc.limit(10)

# COMMAND ----------

# VIEWS
po_headers_all_inc.createOrReplaceTempView('po_headers_all_inc')

# COMMAND ----------

TMP_EXCHANGERATE_VALUES = spark.sql("""
select
    max(GL_DAILY_RATES.CONVERSION_RATE) CONVERSION_RATE,
    POH.PO_HEADER_ID
FROM
  po_headers_all_inc POH
  INNER JOIN EBS.HR_OPERATING_UNITS ON POH.ORG_ID = HR_OPERATING_UNITS.ORGANIZATION_ID
  INNER JOIN EBS.GL_SETS_OF_BOOKS ON HR_OPERATING_UNITS.SET_OF_BOOKS_ID = GL_SETS_OF_BOOKS.SET_OF_BOOKS_ID
  LEFT OUTER JOIN EBS.GL_DAILY_RATES ON POH.CURRENCY_CODE = GL_DAILY_RATES.FROM_CURRENCY
     AND GL_SETS_OF_BOOKS.CURRENCY_CODE = GL_DAILY_RATES.TO_CURRENCY
     AND GL_DAILY_RATES.CONVERSION_DATE <= TO_DATE(DATE_FORMAT(POH.CREATION_DATE, 'yyyyMMdd'),'yyyyMMdd')
WHERE
  1=1
  and COALESCE(GL_DAILY_RATES.CONVERSION_TYPE, 'Corporate') = 'Corporate'
 GROUP BY POH.PO_HEADER_ID
""")
TMP_EXCHANGERATE_VALUES.createOrReplaceTempView("TMP_EXCHANGERATE_VALUES")
TMP_EXCHANGERATE_VALUES.cache()
TMP_EXCHANGERATE_VALUES.count()

# COMMAND ----------

main = spark.sql("""
 select
    REPLACE(STRING(INT(POH.CREATED_BY)), ",", "") AS createdBy,
    POH.CREATION_DATE createdOn,
    REPLACE(STRING(INT(POH.LAST_UPDATED_BY)), ",", "") AS modifiedBy,
    POH.LAST_UPDATE_DATE modifiedOn,
    CURRENT_DATE() insertedOn,
    CURRENT_DATE() updatedOn,
    POH.APPROVED_DATE approvalDate,
    POH.APPROVED_FLAG approvalFlag,
    POH.AUTHORIZATION_STATUS authorizationStatus,
    GL_SETS_OF_BOOKS.CURRENCY_CODE baseCurrencyId,
    REPLACE(STRING(INT(POH.AGENT_ID)), ",", "") AS buyerId,
    POH.CANCEL_FLAG cancelledFlag,
    POH.CLOSED_DATE closedDate,
    POH.CURRENCY_CODE currency,
    POH.END_DATE endDate,
    case when GL_SETS_OF_BOOKS.CURRENCY_CODE = POH.CURRENCY_CODE
      then 1
     ELSE ExchangeRateVal.CONVERSION_RATE
    END exchangeRate,
    POH.RATE_DATE exchangeRateDate,
    case when POH.CURRENCY_CODE = 'USD'
      then 1
      else EXCHANGE_RATE_AGG.exchangeRate
    end exchangeRateUsd,
    POH.FOB_LOOKUP_CODE fobLookupCode,
    POH.FREIGHT_TERMS_LOOKUP_CODE freightTermsLookupCode,
    POH.IN_TRANSIT_TIME inTransitTime,
    POH.SEGMENT1 orderNumber,
    POH.CLOSED_CODE orderStatus,
    REPLACE(STRING(INT(POH.ORG_ID)), ",", "") AS owningBusinessUnitId,
    REPLACE(STRING(INT(POH.PO_HEADER_ID)), ",", "") AS purchaseOrderId,
    POH.REVISED_DATE revisionDate,
    POH.REVISION_NUM revisionNumber,
    POH.SHIP_VIA_LOOKUP_CODE shipViaLookupCode,
    POH.START_DATE startDate,
    REPLACE(STRING(INT(POH.VENDOR_ID)), ",", "") AS supplierId,
    REPLACE(STRING(INT(VENDOR_SITE_ID)),",","") AS supplierSiteId,
    POH.TYPE_LOOKUP_CODE AS typeLookupCode
FROM
  po_headers_all_inc POH
  INNER JOIN EBS.HR_OPERATING_UNITS ON POH.ORG_ID = HR_OPERATING_UNITS.ORGANIZATION_ID
  INNER JOIN EBS.GL_SETS_OF_BOOKS ON HR_OPERATING_UNITS.SET_OF_BOOKS_ID = GL_SETS_OF_BOOKS.SET_OF_BOOKS_ID
  LEFT JOIN TMP_EXCHANGERATE_VALUES ExchangeRateVal ON ExchangeRateVal.PO_HEADER_ID = POH.PO_HEADER_ID
  LEFT OUTER JOIN S_CORE.EXCHANGE_RATE_AGG ON 
      'AVERAGE' || '-' || case when length(trim(POH.CURRENCY_CODE)) = 0 then 'USD' else  trim(POH.CURRENCY_CODE) end || '-USD-' || date_format(POH.APPROVED_DATE, 'yyyy-MM') || '-01-EBS' = EXCHANGE_RATE_AGG.fxID
""")
main.cache()
display(main)

# COMMAND ----------

columns = list(schema.keys())

# COMMAND ----------

main = remove_duplicate_rows(df = main, 
                             key_columns = ['purchaseOrderId'], 
                             tableName = table_name, 
                             sourceName = source_name, 
                             notebookName = NOTEBOOK_NAME, 
                             notebookPath = NOTEBOOK_PATH)

# COMMAND ----------

# TRANSFORM DATA
main_f = (
  main
  .transform(convert_null_to_unknown('buyerId'))
  .transform(tg_default(source_name))
  .transform(tg_supplychain_purchaseorder())
  .transform(attach_surrogate_key(columns = 'purchaseOrderId,_SOURCE'))
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

#HANDLE DELETE
full_keys_f = (
  spark.sql("""
    SELECT
      REPLACE(STRING(INT(PO_HEADER_ID)), ",", "") AS purchaseOrderId
    FROM EBS.PO_HEADERS_ALL 
  """)
  .transform(attach_source_column(source = source_name))
  .transform(attach_surrogate_key(columns = 'purchaseOrderId,_SOURCE'))
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
  cutoff_value = get_incr_col_max_value(po_headers_all_inc)
  update_cutoff_value(cutoff_value, table_name, 'EBS.PO_HEADERS_ALL')
  update_run_datetime(run_datetime, table_name, 'EBS.PO_HEADERS_ALL')

# COMMAND ----------

# MAGIC %run ../_SHARED/PARTIALS/1.2/footer_s_supplychain
