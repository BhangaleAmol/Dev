# Databricks notebook source
# MAGIC %run ../_SHARED/FUNCTIONS/1.2/bootstrap

# COMMAND ----------

# MAGIC %run ../_SHARED/PARTIALS/1.2/header_s_supplychain

# COMMAND ----------

# MAGIC %run ../_SCHEMA/s_supplychain.sales_invoice_headers

# COMMAND ----------

# LOAD DATASETS
if incremental:
  cutoff_value = get_cutoff_value(table_name, 'kgd.dbo_tembo_icsale', prune_days)
  icsale = load_incr_dataset('kgd.dbo_tembo_icsale', 'modifiedOn', cutoff_value)
else:
  icsale = load_full_dataset('kgd.dbo_tembo_icsale')
icsale.createOrReplaceTempView('icsale')

# COMMAND ----------

# SAMPLING
if sampling:
  icsale = icsale.limit(10)
  icsale.createOrReplaceTempView('icsale')

# COMMAND ----------

invoice_type = spark.sql("""
select 
  FInterID, sum(SellQty) totalQuantity
from 
  kgd.dbo_tembo_icsaleentry
where 
  not _DELETED
group by 
  FInterID
""")
invoice_type.createOrReplaceTempView('invoice_type')

# COMMAND ----------

main = spark.sql("""
SELECT 
    NULL AS createdBy,
	icsale.createdOn,
	NULL AS modifiedBy,
	icsale.modifiedOn,
	CURRENT_TIMESTAMP() AS insertedOn,
	CURRENT_TIMESTAMP() AS updatedOn,
	'RMB' AS baseCurrencyId,
	Concat('BILL_TO-', Customerid)  AS billToAddressId,
	NULL AS billToCity,
	NULL AS billToCountry,
	NULL AS billToLine1,
	NULL AS billToLine2,
	NULL AS billToLine3,
	NULL AS billToPostalCode,
	NULL AS billToState,
	'001' AS client,
    NULL AS customerDivision,
	Customerid AS customerId,
	dateInvoiced,
	dateInvoiced AS dateInvoiceRecognition,
	NULL AS distributionChannel,
	1 AS exchangeRate,
    dateInvoiced AS exchangeRateDate,
    'AVERAGE' AS exchangeRateType,
	EXCHANGE_RATE_AGG.exchangeRate AS exchangeRateUSD,
    NULL AS exchangeSpotRate,
	NULL AS freightAmount,
	Invoicenumber AS invoiceId,
	Invoicenumber AS invoiceNumber,
	'AS' AS owningBusinessUnitId,
    NULL AS partyId,
    concat('PAY_TO-',Customerid) AS PayerAddressId,
	NULL AS salesGroup,
	NULL AS salesOffice,
    NULL AS salesOrganization,
    NULL AS salesOrganizationID,
	concat('SHIP_TO-', Customerid) AS shipToAddressId,
	NULL AS shipToCity,
	NULL AS shipToCountry,
	NULL AS shipToLine1,
	NULL AS shipToLine2,
	NULL AS shipToLine3,
	NULL AS shiptoName,
	NULL AS shipToPostalCode,
	NULL AS shipToState,
    concat('SOLD_TO-',Customerid) AS soldToAddressId,
	NULL AS territoryId,
	NULL AS totalAmount,
	NULL AS totalTax,
	transactionCurrency AS transactionCurrencyId,
    case when invoice_type.totalQuantity >= 0 
      then 'INV'
    else 'CM'
    end transactionId
FROM icsale 
  join invoice_type on icsale.fInterID = invoice_type.fInterID
  LEFT OUTER JOIN S_CORE.EXCHANGE_RATE_AGG ON 
      'AVERAGE' || '-' || 'RMB' || '-USD-' || date_format(dateInvoiced, 'yyyy-MM') || '-01-QV' = EXCHANGE_RATE_AGG.fxID
WHERE
   NOT icsale._DELETED
""")

main.createOrReplaceTempView('main')
# print(main.count())


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
  spark.sql("""
    SELECT
      Invoicenumber AS invoiceId,
      'AS' as owningBusinessUnitId
    FROM kgd.dbo_tembo_icsale
    WHERE NOT _DELETED
  """)
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
  cutoff_value = get_incr_col_max_value(icsale)
  update_cutoff_value(cutoff_value, table_name, 'kgd.dbo_tembo_icsale')
  update_run_datetime(run_datetime, table_name, 'kgd.dbo_tembo_icsale')

# COMMAND ----------

# MAGIC %run ../_SHARED/PARTIALS/1.2/footer_s_supplychain
