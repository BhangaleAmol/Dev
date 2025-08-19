# Databricks notebook source
# MAGIC %run ../_SHARED/FUNCTIONS/1.2/bootstrap

# COMMAND ----------

# MAGIC %run ../_SHARED/PARTIALS/1.2/header_s_supplychain

# COMMAND ----------

# MAGIC %run ../_SCHEMA/s_supplychain.sales_invoice_headers

# COMMAND ----------

# LOAD DATASETS
if incremental:
  cutoff_value = get_cutoff_value(table_name, 'ebs.ra_customer_trx_all', prune_days)
  ra_customer_trx_all = load_incr_dataset('ebs.ra_customer_trx_all', 'LAST_UPDATE_DATE', cutoff_value)
else:
  ra_customer_trx_all = load_full_dataset('ebs.ra_customer_trx_all')

# VIEWS
ra_customer_trx_all.createOrReplaceTempView('ra_customer_trx_all')

# COMMAND ----------

# SAMPLING
if sampling:
  ra_customer_trx_all = ra_customer_trx_all.limit(10)
  ra_customer_trx_all.createOrReplaceTempView('ra_customer_trx_all')

# COMMAND ----------

CUSTOMER_LOC_USE_D = spark.sql("""
  SELECT
    HCSUA.SITE_USE_ID, 
    HCSUA.LAST_UPDATED_BY,
    HCSUA.CREATION_DATE,
    HCSUA.CREATED_BY,
    HCA.CUST_ACCOUNT_ID,
    HCSUA.SITE_USE_CODE,
    HCSUA.LOCATION,
    HL.LOCATION_ID,
    HCASA.CUST_ACCT_SITE_ID,
    HPS.PARTY_SITE_ID,
    HCASA.LAST_UPDATE_DATE,
    HCSUA.STATUS,
    HCSUA.TERRITORY_ID
  FROM EBS.HZ_CUST_ACCT_SITES_ALL HCASA
  INNER JOIN EBS.HZ_CUST_SITE_USES_ALL HCSUA ON HCSUA.CUST_ACCT_SITE_ID = HCASA.CUST_ACCT_SITE_ID
  INNER JOIN EBS.HZ_PARTY_SITES HPS ON HCASA.PARTY_SITE_ID = HPS.PARTY_SITE_ID
  INNER JOIN EBS.HZ_CUST_ACCOUNTS HCA ON HCA.CUST_ACCOUNT_ID = HCASA.CUST_ACCOUNT_ID
  INNER JOIN EBS.HZ_LOCATIONS HL ON HPS.LOCATION_ID = HL.LOCATION_ID
""")

CUSTOMER_LOC_USE_D.createOrReplaceTempView("CUSTOMER_LOC_USE_D")
CUSTOMER_LOC_USE_D.cache()
CUSTOMER_LOC_USE_D.count()

# COMMAND ----------

EXCH_RATE = spark.sql("""
 SELECT 
    GL_DAILY_RATES.FROM_CURRENCY, 
    GL_DAILY_RATES.TO_CURRENCY, 
    GL_DAILY_RATES.CONVERSION_TYPE, 
    CAST(DATE_FORMAT(GL_DAILY_RATES.CONVERSION_DATE,"yyyyMMdd") AS INT) CONVERSION_DATE,
    GL_DAILY_RATES.FROM_CURRENCY FROM_CURCY_CD,
    GL_DAILY_RATES.TO_CURRENCY TO_CURCY_CD,
    GL_DAILY_RATES.CONVERSION_TYPE RATE_TYPE,
    GL_DAILY_RATES.CONVERSION_DATE START_DT,
    DATE_ADD(GL_DAILY_RATES.CONVERSION_DATE,1) END_DT,
    GL_DAILY_RATES.CONVERSION_RATE EXCH_RATE,
    GL_DAILY_RATES.CONVERSION_DATE EXCH_DT
  FROM 
    EBS.GL_DAILY_RATES
    WHERE GL_DAILY_RATES.FROM_CURRENCY = 'USD'
    AND  UPPER(GL_DAILY_RATES.CONVERSION_TYPE) = 'CORPORATE'
""")

EXCH_RATE.createOrReplaceTempView("EXCH_RATE")

# COMMAND ----------

main = spark.sql("""
SELECT DISTINCT 
    REPLACE(
    STRING(INT(i.CREATED_BY)) ,
    ",",
    ""
  ) AS createdBy,
	i.CREATION_DATE AS createdOn,
	    REPLACE(
    STRING(INT(i.LAST_UPDATED_BY)) ,
    ",",
    ""
  ) AS modifiedBy,
	i.LAST_UPDATE_DATE AS modifiedOn,
	CURRENT_TIMESTAMP() AS insertedOn,
	CURRENT_TIMESTAMP() AS updatedOn,
	gsob.CURRENCY_CODE  AS baseCurrencyId,
	BILL_TO.SITE_USE_CODE || '-' ||  REPLACE(
    STRING(INT(BILL_TO.SITE_USE_ID)) ,
    ",",
    ""
  ) AS billToAddressId,
	CAST(NULL as string) AS billToCity,
	CAST(NULL as string) AS billToCountry,
	CAST(NULL as string) AS billToLine1,
	CAST(NULL as string) AS billToLine2,
	CAST(NULL as string) AS billToLine3,
	CAST(NULL as string) AS billToPostalCode,
	CAST(NULL as string) AS billToState,
	'001' AS client,
    CAST(NULL as string) AS customerDivision,
	    REPLACE(
    STRING(INT(i.BILL_TO_CUSTOMER_ID)) ,
    ",",
    ""
  ) AS customerId,
	i.TRX_DATE AS dateInvoiced,
	i.TRX_DATE AS dateInvoiceRecognition,
	CAST(NULL as string) AS distributionChannel,
	--RCTTA.NAME AS documentType,
	(CASE WHEN i.INVOICE_CURRENCY_CODE  = gsob.CURRENCY_CODE THEN 1
        ELSE i.EXCHANGE_RATE END) AS exchangeRate,
     NVL(i.EXCHANGE_DATE,i.TRX_DATE) exchangeRateDate,
     i.EXCHANGE_RATE_TYPE exchangeRateType,
	CASE 
      WHEN i.INVOICE_CURRENCY_CODE = 'USD' THEN 1
      ELSE 
        COALESCE(EXCHANGE_RATE_AGG.exchangeRate, 1)
    END AS exchangeRateUSD,
    CASE 
     WHEN i.INVOICE_CURRENCY_CODE  = 'USD' THEN 1
     WHEN i.INVOICE_CURRENCY_CODE  = gsob.CURRENCY_CODE THEN NVL(i.EXCHANGE_RATE,EXCH_RATE.EXCH_RATE)
     ELSE EXCH_RATE.EXCH_RATE END exchangeSpotRate,
	CAST(NULL AS decimal(22,7)) AS freightAmount,
	    REPLACE(
    STRING(INT(i.CUSTOMER_TRX_ID)) ,
    ",",
    ""
  ) AS invoiceId,
	i.TRX_NUMBER AS invoiceNumber,
	    REPLACE(
    STRING(INT(i.ORG_ID)) ,
    ",",
    ""
  ) AS owningBusinessUnitId,
    REPLACE(
    STRING(INT(HCA.PARTY_ID)),
    ",",
    ""
   )AS partyId,
   CAST(NULL as string) AS PayerAddressId,
	CAST(NULL as string) AS salesGroup,
	CAST(NULL as string) AS salesOffice,
    CAST(NULL as string) AS salesOrganization,
    CAST(NULL as string) AS salesOrganizationID,
	SHIP_TO.SITE_USE_CODE || '-' || REPLACE(
    STRING(INT(SHIP_TO.SITE_USE_ID)) ,
    ",",
    ""
  ) AS shipToAddressId,
	CAST(NULL AS STRING) AS shipToCity,
	CAST(NULL AS STRING) AS shipToCountry,
	CAST(NULL AS STRING) AS shipToLine1,
	CAST(NULL AS STRING) AS shipToLine2,
	CAST(NULL AS STRING) AS shipToLine3,
	CAST(NULL AS STRING) AS shiptoName,
	CAST(NULL AS STRING) AS shipToPostalCode,
	CAST(NULL AS STRING) AS shipToState,
    BILL_TO.SITE_USE_CODE || '-' ||  REPLACE(
    STRING(INT(BILL_TO.SITE_USE_ID)) ,
    ",",
    ""
  )  AS soldToAddressId,
	CAST(NULL AS STRING) AS territoryId,
	CAST(NULL AS decimal(22,7)) AS totalAmount,
	CAST(NULL AS decimal(22,7)) AS totalTax,
	i.INVOICE_CURRENCY_CODE  AS transactionCurrencyId,
    REPLACE(
    STRING(INT (i.CUST_TRX_TYPE_ID)),
    ",",
    ""
  ) || '-' || REPLACE(
    STRING(INT (i.ORG_ID)),
    ",",
    ""
  ) transactionId
FROM RA_CUSTOMER_TRX_ALL i
 INNER JOIN EBS.RA_CUSTOMER_TRX_LINES_ALL RCTLA ON i.CUSTOMER_TRX_ID = RCTLA.CUSTOMER_TRX_ID
 --INNER JOIN EBS.RA_CUST_TRX_TYPES_ALL RCTTA ON i.CUST_TRX_TYPE_ID = RCTTA.CUST_TRX_TYPE_ID AND i.ORG_ID = RCTTA.ORG_ID
 LEFT JOIN EBS.GL_SETS_OF_BOOKS gsob ON i.SET_OF_BOOKS_ID = gsob.SET_OF_BOOKS_ID
 LEFT JOIN CUSTOMER_LOC_USE_D SHIP_TO ON i.SHIP_TO_SITE_USE_ID = SHIP_TO.SITE_USE_ID
  LEFT JOIN CUSTOMER_LOC_USE_D BILL_TO ON i.BILL_TO_SITE_USE_ID = BILL_TO.SITE_USE_ID
  LEFT JOIN EBS.HZ_CUST_ACCOUNTS HCA ON i.BILL_TO_CUSTOMER_ID = HCA.cust_account_id
 LEFT OUTER JOIN S_CORE.EXCHANGE_RATE_AGG ON 
      'AVERAGE' || '-' || case when length(trim(i.INVOICE_CURRENCY_CODE)) = 0 then 'USD' else  trim(i.INVOICE_CURRENCY_CODE) end || '-USD-' || date_format(COALESCE(i.EXCHANGE_DATE,i.TRX_DATE), 'yyyy-MM') || '-01-QV' = EXCHANGE_RATE_AGG.fxID
    LEFT JOIN EXCH_RATE EXCH_RATE ON
   i.INVOICE_CURRENCY_CODE = EXCH_RATE.TO_CURCY_CD
  AND coalesce(i.EXCHANGE_DATE, i.TRX_DATE) >= EXCH_RATE.START_DT
  AND coalesce(i.EXCHANGE_DATE, i.TRX_DATE) < EXCH_RATE.END_DT

""")

main.cache()
# display(main)

# COMMAND ----------

columns = list(schema.keys())

# COMMAND ----------

file_name = get_file_name(table_name, 'dlt')
target_file = get_file_path(temp_folder, file_name, target_container, target_storage)

main.write.format('delta').mode('overwrite').option('overwriteSchema', 'true').save(target_file)

main_tempDF = spark.read.format('delta').load(target_file)

# COMMAND ----------

main = remove_duplicate_rows(df = main_tempDF, 
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
      REPLACE(STRING(INT(CUSTOMER_TRX_ID)), ",", "") AS invoiceId,
      REPLACE(STRING(INT(ORG_ID)), ",", "") AS owningBusinessUnitId
    FROM ebs.ra_customer_trx_all
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
  cutoff_value = get_incr_col_max_value(ra_customer_trx_all, 'LAST_UPDATE_DATE')
  update_cutoff_value(cutoff_value, table_name, 'ebs.ra_customer_trx_all')
  update_run_datetime(run_datetime, table_name, 'ebs.ra_customer_trx_all')

# COMMAND ----------

# MAGIC %run ../_SHARED/PARTIALS/1.2/footer_s_supplychain
