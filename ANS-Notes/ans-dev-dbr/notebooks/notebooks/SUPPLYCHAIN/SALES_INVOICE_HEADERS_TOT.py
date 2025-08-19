# Databricks notebook source
# MAGIC %run ../_SHARED/FUNCTIONS/1.2/bootstrap

# COMMAND ----------

# MAGIC %run ../_SHARED/PARTIALS/1.2/header_s_supplychain

# COMMAND ----------

# MAGIC %run ../_SCHEMA/s_supplychain.sales_invoice_headers

# COMMAND ----------

# LOAD DATASETS
tb_pbi_invoices = load_full_dataset('tot.tb_pbi_invoices')
tb_pbi_invoices.createOrReplaceTempView('tb_pbi_invoices')

tb_pbi_returns = load_full_dataset('tot.tb_pbi_returns')
tb_pbi_returns.createOrReplaceTempView('tb_pbi_returns')

# COMMAND ----------

# SAMPLING
if sampling:
  tb_pbi_invoices = tb_pbi_invoices.limit(10)
  tb_pbi_invoices.createOrReplaceTempView('tb_pbi_invoices')
  
  tb_pbi_returns = tb_pbi_returns.limit(10)
  tb_pbi_returns.createOrReplaceTempView('tb_pbi_returns')

# COMMAND ----------

tb_pbi_returns_agg = spark.sql("""
SELECT DISTINCT
    NULL AS createdBy,
	NULL AS createdOn,
	NULL AS modifiedBy,
	NULL AS modifiedOn,
	CURRENT_TIMESTAMP() AS insertedOn,
	CURRENT_TIMESTAMP() AS updatedOn,
	'BRL' AS baseCurrencyId,
	Concat('BILL_TO-',i.Customer_id)  AS billToAddressId,
	NULL AS billToCity,
	NULL AS billToCountry,
	NULL AS billToLine1,
	NULL AS billToLine2,
	NULL AS billToLine3,
	NULL AS billToPostalCode,
	NULL AS billToState,
	'001' AS client,
    NULL AS customerDivision,
	CONCAT(r.Company, '-', r.Customer_id) AS customerId,
	r.Entry_date AS dateInvoiced,
	r.Entry_date AS dateInvoiceRecognition,
	NULL AS distributionChannel,
	--'CM' AS documentType,
	1 AS exchangeRate,
    NULL AS exchangeRateDate,
    NULL AS exchangeRateType,
	EXCHANGE_RATE_AGG.exchangeRate AS exchangeRateUSD,
    NULL AS exchangeSpotRate,
	NULL AS freightAmount,
	CONCAT (r.Company,'-',r.Invoice_number,'-',r.Invoice_number_origin) AS invoiceId,
	r.Invoice_number AS invoiceNumber,
	r.Company AS owningBusinessUnitId,
    NULL AS partyId,
    concat('PAY_TO-',i.Customer_id) AS PayerAddressId,
	NULL AS salesGroup,
	NULL AS salesOffice,
    NULL AS salesOrganization,
    i.TSM_cod AS salesOrganizationId,
	concat('SHIP_TO-',i.Customer_delivery) AS shipToAddressId,
	NULL AS shipToCity,
	NULL AS shipToCountry,
	NULL AS shipToLine1,
	NULL AS shipToLine2,
	NULL AS shipToLine3,
	NULL AS shiptoName,
	NULL AS shipToPostalCode,
	NULL AS shipToState,
    concat('SOLD_TO-',i.Customer_id) AS soldToAddressId,
	i.TSM_cod AS territoryId,
	NULL AS totalAmount,
	NULL AS totalTax,
	'BRL' AS transactionCurrencyId,
    'CM' AS transactionId
FROM tb_pbi_returns r
LEFT JOIN tot.tb_pbi_invoices i ON r.Invoice_number_origin = i.Invoice_number AND r.Item_Origin = i.Item AND r.Company = i.Company
LEFT OUTER JOIN S_CORE.EXCHANGE_RATE_AGG ON 
      'AVERAGE' || '-' || 'BRL' || '-USD-' || date_format(r.Entry_date, 'yyyy-MM') || '-01-QV' = EXCHANGE_RATE_AGG.fxID
  
""")

tb_pbi_returns_agg.createOrReplaceTempView('tb_pbi_returns_agg')
print(tb_pbi_returns.count())
print(tb_pbi_returns_agg.count())

# COMMAND ----------

tb_pbi_invoices_agg = spark.sql("""
  SELECT DISTINCT
    NULL AS createdBy,
	NULL AS createdOn,
	NULL AS modifiedBy,
	NULL AS modifiedOn,
	CURRENT_TIMESTAMP() AS insertedOn,
	CURRENT_TIMESTAMP() AS updatedOn,
	'BRL' AS baseCurrencyId,
	Concat('BILL_TO-',i.Customer_id)  AS billtoAddressId,
	NULL AS billtoCity,
	NULL AS billToCountry,
	NULL AS billToLine1,
	NULL AS billToLine2,
	NULL AS billToLine3,
	NULL AS billToPostalCode,
	NULL AS billToState,
	'001' AS client,
    NULL AS customerDivision,
	CONCAT(i.Company, '-', i.Customer_id) AS customerId,
	i.Invoice_date AS dateInvoiced,
	i.Invoice_rec_date AS dateInvoiceRecognition,
	NULL AS distributionChannel,
	--'Invoice' AS documentType,
	1 AS exchangeRate,
    NULL AS exchangeRateDate,
    NULL AS exchangeRateType,
	EXCHANGE_RATE_AGG.exchangeRate AS exchangeRateUSD,
    NULL AS exchangeSpotRate,
	NULL AS freightAmount,
	CONCAT (
		i.Company,
		'-',
		i.Invoice_number
		) AS invoiceId,
	i.Invoice_number AS invoiceNumber,
	i.Company AS owningBusinessUnitId,
    NULL AS partyId,
    concat('PAY_TO-',i.Customer_id) AS PayerAddressId,
	NULL AS salesGroup,
	NULL AS salesOffice,
    NULL AS salesOrganization,
    i.TSM_cod AS salesOrganizationID,
	concat('SHIP_TO-',i.Customer_delivery) AS shipToAddressId,
	NULL AS shipToCity,
	NULL AS shipToCountry,
	NULL AS shipToLine1,
	NULL AS shipToLine2,
	NULL AS shipToLine3,
	NULL AS shiptoName,
	NULL AS shipToPostalCode,
	NULL AS shipToState,
    concat('SOLD_TO-',i.Customer_id) AS soldToAddressId,
	i.TSM_cod AS territoryId,
	NULL AS totalAmount,
	NULL AS totalTax,
	'BRL' AS transactionCurrencyId,
    'INV' AS transactionId
FROM tb_pbi_invoices i 
LEFT JOIN S_CORE.EXCHANGE_RATE_AGG ON 
      'AVERAGE' || '-' || 'BRL' || '-USD-' || date_format(i.Invoice_date, 'yyyy-MM') || '-01-QV' = EXCHANGE_RATE_AGG.fxID
""")

tb_pbi_invoices_agg.createOrReplaceTempView('tb_pbi_invoices_agg')
print(tb_pbi_invoices.count())
print(tb_pbi_invoices_agg.count())

# COMMAND ----------

main = spark.sql("""
  SELECT * FROM tb_pbi_returns_agg
  UNION
  SELECT * FROM tb_pbi_invoices_agg
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
keys_ret_f = (
  spark.sql("""
    SELECT
      CONCAT(Company, '-', Invoice_number, '-', Invoice_number_origin) AS invoiceId,
      Company AS owningBusinessUnitId
    FROM tot.tb_pbi_returns
    WHERE _DELETED IS FALSE
  """)
)

keys_inv_f = (
  spark.sql("""
    SELECT
      CONCAT(Company, '-', Invoice_number) AS invoiceId,
      Company AS owningBusinessUnitId
    FROM tb_pbi_invoices
    WHERE _DELETED IS FALSE
  """)
)

full_keys_f = (
  keys_ret_f
  .union(keys_inv_f)
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
  cutoff_value = get_incr_col_max_value(tb_pbi_invoices)
  update_cutoff_value(cutoff_value, table_name, 'tot.tb_pbi_invoices')
  update_run_datetime(run_datetime, table_name, 'tot.tb_pbi_invoices')

# COMMAND ----------

# MAGIC %run ../_SHARED/PARTIALS/1.2/footer_s_supplychain
