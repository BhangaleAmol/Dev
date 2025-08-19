# Databricks notebook source
# MAGIC %run ../_SHARED/FUNCTIONS/1.2/bootstrap

# COMMAND ----------

# MAGIC %run ../_SHARED/PARTIALS/1.2/header_s_supplychain

# COMMAND ----------

# MAGIC %run ../_SCHEMA/s_supplychain.sales_invoice_headers

# COMMAND ----------

# LOAD DATASETS
vw_qv_invoices = load_full_dataset('col.vw_qv_invoices')

# COMMAND ----------

# SAMPLING
if sampling:
  vw_qv_invoices = vw_qv_invoices.limit(10)

# COMMAND ----------

# VIEWS
vw_qv_invoices.createOrReplaceTempView('vw_qv_invoices')

# COMMAND ----------

main = spark.sql("""
SELECT DISTINCT NULL AS createdBy,
	NULL AS createdOn,
	NULL AS modifiedBy,
	NULL AS modifiedOn,
	CURRENT_TIMESTAMP() AS insertedOn,
	CURRENT_TIMESTAMP() AS updatedOn,
	i.Currency AS baseCurrencyId,
	Concat('BILL_TO-',i.CustomerId) AS billToAddressId,
	NULL AS billToCity,
	NULL AS billToCountry,
	NULL AS billToLine1,
	NULL AS billToLine2,
	NULL AS billToLine3,
	NULL AS billToPostalCode,
	NULL AS billToState,
	'001' AS client,
    NULL AS customerDivision,
	CONCAT(i.Company, '-', i.CustomerId)  AS customerId,
	TO_DATE(i.Invoice_Date, 'mm/dd/yyyy') AS dateInvoiced,
	TO_DATE(i.Invoice_Date, 'mm/dd/yyyy') AS dateInvoiceRecognition,
	NULL AS distributionChannel,
	--i.Doc_Type AS documentType,
	1 AS exchangeRate,
    NULL AS exchangeRateDate,
    NULL AS exchangeRateType,
    EXCHANGE_RATE_AGG.exchangeRate exchangeRateUSD,
    NULL AS exchangeSpotRate,
	NULL AS freightAmount,
	i.Invoice_Number AS invoiceId,
	i.Invoice_Number AS invoiceNumber,
	i.Company AS owningBusinessUnitId,
    NULL AS partyId,
    concat('PAY_TO-',i.CustomerId) AS PayerAddressId,
	NULL AS salesGroup,
	NULL AS salesOffice,
    NULL AS salesOrganization,
    NULL AS salesOrganizationID,
	concat('SHIP_TO-',i.ShipToDeliveryLocationID) AS shipToAddressId,
	NULL AS shipToCity,
	NULL AS shipToCountry,
	NULL AS shipToLine1,
	NULL AS shipToLine2,
	NULL AS shipToLine3,
	NULL AS shiptoName,
	NULL AS shipToPostalCode,
	NULL AS shipToState,
    concat('SOLD_TO-',i.CustomerId) AS soldToAddressId,
	NULL AS territoryId,
	NULL AS totalAmount,
	NULL AS totalTax,
	'COP' AS transactionCurrencyId,
    i.Doc_Type || '-' || i.Company  AS transactionId
FROM vw_qv_invoices i
LEFT JOIN S_CORE.EXCHANGE_RATE_AGG ON 
      'AVERAGE' || '-' || case when length(trim(i.CURRENCY)) = 0 then 'COL' else  trim(i.Currency) end || '-USD-' || date_format(i.Invoice_Date, 'yyyy-MM') || '-01-QV' = EXCHANGE_RATE_AGG.fxID

""")

main.cache()
display(main)

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
full_keys_f = (
  spark.table('col.vw_qv_invoices')
  .filter('_DELETED IS FALSE')
  .selectExpr("Invoice_Number AS invoiceId", "Company AS owningBusinessUnitId")
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
  cutoff_value = get_incr_col_max_value(vw_qv_invoices)
  update_cutoff_value(cutoff_value, table_name, 'col.vw_qv_invoices')
  update_run_datetime(run_datetime, table_name, 'col.vw_qv_invoices')

# COMMAND ----------

# MAGIC %run ../_SHARED/PARTIALS/1.2/footer_s_supplychain
