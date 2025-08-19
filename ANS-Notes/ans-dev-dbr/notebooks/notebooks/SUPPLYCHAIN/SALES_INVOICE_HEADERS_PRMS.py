# Databricks notebook source
# MAGIC %run ../_SHARED/FUNCTIONS/1.2/bootstrap

# COMMAND ----------

# MAGIC %run ../_SHARED/PARTIALS/1.2/header_s_supplychain

# COMMAND ----------

# MAGIC %run ../_SCHEMA/s_supplychain.sales_invoice_headers

# COMMAND ----------

# LOAD DATASETS
if incremental:
  cutoff_value = get_cutoff_value(table_name, 'prms.prmsf200_obirp111', prune_days)
  prmsf200_obirp111 = load_incr_dataset('prms.prmsf200_obirp111', 'TADCD', cutoff_value)
  
  cutoff_value = get_cutoff_value(table_name, 'prms.prmsf200_obcdp100', prune_days)
  prmsf200_obcdp100 = load_incr_dataset('prms.prmsf200_obcdp100', 'CMRDT', cutoff_value)
  
else:
  prmsf200_obirp111 = load_full_dataset('prms.prmsf200_obirp111')
  prmsf200_obcdp100 = load_full_dataset('prms.prmsf200_obcdp100')
  
  
# VIEWS
prmsf200_obirp111.createOrReplaceTempView('prmsf200_obirp111')
prmsf200_obcdp100.createOrReplaceTempView('prmsf200_obcdp100')

# COMMAND ----------

# SAMPLING
if sampling:
  prmsf200_obirp111 = prmsf200_obirp111.limit(10)
  prmsf200_obirp111.createOrReplaceTempView('prmsf200_obirp111')
  
  prmsf200_obcdp100 = prmsf200_obcdp100.limit(10)
  prmsf200_obcdp100.createOrReplaceTempView('prmsf200_obcdp100')

# COMMAND ----------

invoices = spark.sql("""
SELECT 
    OBIRP111.IRLUS AS createdBy,
	OBIRP111.TADCD AS createdOn,
	OBIRP111.IRLUS AS modifiedBy,
	OBIRP111.TADCD AS modifiedOn,
	CURRENT_TIMESTAMP() AS insertedOn,
	CURRENT_TIMESTAMP() AS updatedOn,
	'AUD' AS baseCurrencyId,
	REPLACE(STRING(INT(OBIRP111.CUSNO)), ",", "")  AS billToAddressId,
	NULL AS billToCity,
	NULL AS billToCountry,
	NULL AS billToLine1,
	NULL AS billToLine2,
	NULL AS billToLine3,
	NULL AS billToPostalCode,
	NULL AS billToState,
	'001' AS client,
    NULL AS customerDivision,
	REPLACE(STRING(INT(OBIRP111.CMPNO)), ",", "") || '-' || REPLACE(STRING(INT(OBIRP111.CUSNO)), ",", "")  AS customerId,
	OBIRP111.TTDDT AS dateInvoiced,
	OBIRP111.TTDDT AS dateInvoiceRecognition,
	NULL AS distributionChannel,
	--'Invoice' AS documentType,
	OBIRP111.IEXRT AS exchangeRate,
    NULL AS exchangeRateDate,
    NULL AS exchangeRateType,
    EXCHANGE_RATE_AGG.exchangeRate exchangeRateUSD,
    NULL AS exchangeSpotRate,
	NULL AS freightAmount,
	REPLACE(STRING(INT(OBIRP111.INVNO)), ",", "") AS invoiceId,
	REPLACE(STRING(INT(OBIRP111.INVNO)), ",", "") AS invoiceNumber,
	REPLACE(STRING(INT(OBIRP111.CMPNO)), ",", "") AS owningBusinessUnitId,
    NULL AS partyId,
    NULL AS PayerAddressId,
	NULL AS salesGroup,
	NULL AS salesOffice,
    NULL AS salesOrganization,
    NULL AS salesOrganizationID,
	REPLACE(STRING(INT(OBIRP111.SHPNO)), ",", "") AS shipToAddressId,
	NULL AS shipToCity,
	NULL AS shipToCountry,
	TRIM(OBIRP111.INAD1) AS shipToLine1,
	TRIM(OBIRP111.INAD2) AS shipToLine2,
	TRIM(OBIRP111.INADX) || ' ' || TRIM(OBIRP111.INAD3) AS shipToLine3,
	TRIM(OBIRP111.INNAM) AS shiptoName,
	TRIM(OBIRP111.INZIP) AS shipToPostalCode,
	TRIM(OBIRP111.INSTA) AS shipToState,
    NULL AS soldToAddressId,
	NULL AS territoryId,
	NULL AS totalAmount,
	NULL AS totalTax,
	CASE WHEN LENGTH(TRIM(OBIRP111.ICRCD)) = 0 then  'AUD' else TRIM(OBIRP111.ICRCD) END    AS transactionCurrencyId,
    'INV' AS transactionId
FROM prmsf200_obirp111 obirp111
LEFT JOIN PRMS.PRMSF200_OBCOP100 OBCOP100 ON obirp111.ORDNO = OBCOP100.ORDNO
LEFT OUTER JOIN S_CORE.EXCHANGE_RATE_AGG ON
      ('AVERAGE' || '-' || case when length(trim(OBIRP111.ICRCD)) = 0 then 'AUD' else  trim(OBIRP111.ICRCD) end || '-USD-' || date_format(OBIRP111.TTDDT, 'yyyy-MM') || '-01-QV') = EXCHANGE_RATE_AGG.fxID 
      
where obirp111.invno <> 0
""")

invoices.createOrReplaceTempView('invoices')
print(invoices.count())

# COMMAND ----------

credit_notes = spark.sql("""
SELECT 
    OBCDP100.ENTUS AS createdBy,
	OBCDP100.CMRDT AS createdOn,
	OBCDP100.ENTUS AS modifiedBy,
	OBCDP100.CMRDT AS modifiedOn,
	CURRENT_TIMESTAMP() AS insertedOn,
	CURRENT_TIMESTAMP() AS updatedOn,
	'AUD' AS baseCurrencyId,
	REPLACE(STRING(INT(OBCDP100.CUSNO)), ",", "")  AS billtoAddressId,
	NULL AS billtoCity,
	NULL AS billToCountry,
	NULL AS billToLine1,
	NULL AS billToLine2,
	NULL AS billToLine3,
	NULL AS billToPostalCode,
	NULL AS billToState,
	'001' AS client,
    NULL AS customerDivision,
    REPLACE(STRING(INT(OBCDP100.CMPNO)), ",", "") || '-' || REPLACE(STRING(INT(OBCDP100.CUSNO)), ",", "")  AS customerId,
	OBCDP100.CMRDT AS dateInvoiced,
	OBCDP100.CMRDT AS dateInvoiceRecognition,
	NULL AS distributionChannel,
	--CASE WHEN OBCDP100.CMTYP = 'C' THEN 'CM' ELSE 'DM' END AS documentType,
	OBCDP100.CMEXR AS exchangeRate,
    NULL AS exchangeRateDate,
    NULL AS exchangeRateType,
    EXCHANGE_RATE_AGG.exchangeRate exchangeRateUSD,
    NULL AS exchangeSpotRate,
	NULL AS freightAmount,
	REPLACE(STRING(INT(OBCDP100.MEMNO)), ",", "") AS invoiceId,
	REPLACE(STRING(INT(OBCDP100.MEMNO)), ",", "") AS invoiceNumber,
	REPLACE(STRING(INT(OBCDP100.CMPNO)), ",", "") AS owningBusinessUnitId,
    NULL AS partyId,
    NULL AS PayerAddressId,
	NULL AS salesGroup,
	NULL AS salesOffice,
    NULL AS salesOrganization,
    NULL AS salesOrganizationID,
	REPLACE(STRING(INT(OBCDP100.SHPNO)), ",", "")   AS shipToAddressId,
	NULL AS shipToCity,
	NULL AS shipToCountry,
	NULL AS shipToLine1,
	NULL AS shipToLine2,
	NULL AS shipToLine3,
	NULL AS shiptoName,
	NULL AS shipToPostalCode,
	NULL AS shipToState,
    NULL AS soldToAddressId,
	NULL AS territoryId,
	NULL AS totalAmount,
	NULL AS totalTax,
    CASE WHEN LENGTH(TRIM(OBCDP100.CMCCD)) = 0 then  'AUD' else TRIM(OBCDP100.CMCCD) END AS transactionCurrencyId,
    'CM' AS transactionId
FROM prmsf200_obcdp100 obcdp100
LEFT OUTER JOIN S_CORE.EXCHANGE_RATE_AGG ON
      ('AVERAGE' || '-' || case when length(trim(OBCDP100.CMCCD)) = 0 then 'AUD' else  trim(OBCDP100.CMCCD) end || '-USD-' || date_format(OBCDP100.CMRDT, 'yyyy-MM') || '-01-QV') = EXCHANGE_RATE_AGG.fxID 
""")

credit_notes.createOrReplaceTempView('credit_notes')
print(credit_notes.count())

# COMMAND ----------

main = spark.sql("""
  SELECT * FROM invoices
  UNION
  SELECT * FROM credit_notes
""")

# COMMAND ----------

columns = list(schema.keys())

# COMMAND ----------

main = remove_duplicate_rows(df = main, 
                             key_columns = ['invoiceId','owningBusinessUnitId'], 
                             tableName = table_name, 
                             sourceName = source_name, 
                             notebookName = NOTEBOOK_NAME, 
                             notebookPath = NOTEBOOK_PATH)

# COMMAND ----------

# TRANSFORM DATA
main_f = (
  main
  .withColumn('invoiceType',f.lit('INVOICE_TYPE'))
  .transform(tg_default(source_name))
  .transform(tg_supplychain_sales_invoice_headers())
  .drop('invoiceType')
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
keys_obirp111_f = (
  spark.sql("""
    SELECT
      REPLACE(STRING(INT(INVNO)), ",", "") AS invoiceId,
      REPLACE(STRING(INT(CMPNO)), ",", "") AS owningBusinessUnitId
    FROM prms.prmsf200_obirp111
    WHERE invno <> 0
  """)
  .filter("_DELETED IS FALSE")
)

keys_obcdp100_f = (
  spark.sql("""
    SELECT
      REPLACE(STRING(INT(MEMNO)), ",", "") AS invoiceId,
      REPLACE(STRING(INT(CMPNO)), ",", "") AS owningBusinessUnitId
    FROM prms.prmsf200_obcdp100
  """)
  .filter("_DELETED IS FALSE")
)

full_keys_f = (
  keys_obirp111_f
  .union(keys_obcdp100_f)
  .transform(attach_source_column(source = source_name))
  .transform(attach_surrogate_key(columns = 'invoiceId,owningBusinessUnitId,_SOURCE'))
  .select('_ID')
  .transform(add_unknown_ID())
)

apply_soft_delete(full_keys_f, table_name, key_columns = '_ID')

# COMMAND ----------

# UPDATE FK
update_foreign_key(table_name, 'createdBy,_SOURCE', 'createdBy_ID', 'edm.user')
update_foreign_key(table_name, 'modifiedBy,_SOURCE', 'modifiedBy_ID', 'edm.user')
update_foreign_key(table_name, 'customerId,_SOURCE', 'customer_ID', 'edm.account')
update_foreign_key(table_name, 'customerId, customerDivision, salesOrganization, distributionChannel,_SOURCE', 'customerOrganization_ID', 'edm.account_organization')

# COMMAND ----------

# UPDATE CUTOFF VALUE
if not test_run:
  
  cutoff_value = get_incr_col_max_value(prmsf200_obirp111)
  update_cutoff_value(cutoff_value, table_name, 'prms.prmsf200_obirp111')
  update_run_datetime(run_datetime, table_name, 'prms.prmsf200_obirp111')

  cutoff_value = get_incr_col_max_value(prmsf200_obcdp100)
  update_cutoff_value(cutoff_value, table_name, 'prms.prmsf200_obcdp100')
  update_run_datetime(run_datetime, table_name, 'prms.prmsf200_obcdp100')

# COMMAND ----------

# MAGIC %run ../_SHARED/PARTIALS/1.2/footer_s_supplychain
