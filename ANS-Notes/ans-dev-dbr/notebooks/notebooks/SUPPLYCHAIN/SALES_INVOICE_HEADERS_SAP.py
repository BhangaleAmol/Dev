# Databricks notebook source
# MAGIC %run ../_SHARED/FUNCTIONS/1.2/bootstrap

# COMMAND ----------

# MAGIC %run ../_SHARED/PARTIALS/1.2/header_s_supplychain

# COMMAND ----------

# MAGIC %run ../_SCHEMA/s_supplychain.sales_invoice_headers

# COMMAND ----------

# LOAD DATASETS
if incremental:
  cutoff_value = get_cutoff_value(table_name, 'sapp01.VBRK', prune_days)
  salesinvoices = load_incr_dataset('sapp01.VBRK', '_MODIFIED', cutoff_value)
else:
  salesinvoices = load_full_dataset('sapp01.VBRK')
  
# VIEWS  
salesinvoices.createOrReplaceTempView('VBRK')

# COMMAND ----------

# SAMPLING
if sampling:
  salesinvoices = salesinvoices.limit(10)
  salesinvoices.createOrReplaceTempView('VBRK')

# COMMAND ----------

# SHIP_TO_LKP = spark.sql("""
# select DISTINCT
#   shipto.KUNNR,VBRP.VBELN, shipTo.MANDT
# from sapp01.VBRk 
# INNER join sapp01.VBRP on VBRK.VBELN = VBRP.VBELN and VBRK.MANDT = VBRP.MANDT 
# INNER join sapp01.vbpa shipto on VBRP.AUBEL = shipto.vbeln and shipTo.MANDT = VBRP.MANDT --and VBRP.posnr = shipto.posnr
#       and shipto.parvw = 'WE' AND shipto.posnr = VBRP.AUPOS
# where year(VBRK.FKDAT) >= 2014 -- exclude records prior to Ansell integration
# and VBRK.FKTYP not in ('I') -- exclude intercompany records
# and VBRK.FKSTO not in ('X') -- exclude deleted documents
# and VBRK.FKART not in ('S1') -- exclude deleting documents
# """)
# SHIP_TO_LKP.createOrReplaceTempView("SHIP_TO_LKP")
SHIP_TO_LKP = spark.sql("""
select
  MANDT,
  VBELN,
  KUNNR
from
  sapp01.vbpa
where 1=1
and parvw = 'WE'
and vbpa.POSNR = '000000'
and NVL(vbpa._deleted,0) = 0
""")
SHIP_TO_LKP.createOrReplaceTempView("SHIP_TO_LKP")

# COMMAND ----------

BILL_TO_LKP = spark.sql("""
select DISTINCT
  billto.KUNNR,VBRK.VBELN, billTo.MANDT
from sapp01.VBRk 
INNER join sapp01.vbpa billto on VBRK.VBELN = billto.vbeln and billTo.MANDT = VBRK.MANDT 
      and billto.parvw = 'RE' AND billto.posnr = '000000'
      and NVL(vbrk._deleted,0) = 0
      and NVL(billto._deleted,0) = 0
""")
BILL_TO_LKP.createOrReplaceTempView("BILL_TO_LKP")

# COMMAND ----------

SOLD_TO_LKP = spark.sql("""
select DISTINCT
  soldto.KUNNR,VBRK.VBELN, soldTo.MANDT
from sapp01.VBRK 
-- INNER join sapp01.VBRP on VBRK.VBELN = VBRP.VBELN and VBRK.MANDT = VBRP.MANDT 
INNER join sapp01.vbpa soldTo on VBRK.VBELN = soldTo.vbeln and soldTo.MANDT = VBRK.MANDT --and VBRP.posnr = soldto.posnr
      and soldTo.parvw = 'AG' AND soldTo.posnr = '000000'
      and NVL(soldTo._deleted,0) = 0
      and NVL(vbrk._deleted,0) = 0
""")
SOLD_TO_LKP.createOrReplaceTempView("SOLD_TO_LKP")

# COMMAND ----------

exclude_proforma = spark.sql("""
  select 
    mandt, 
    fkart 
  from 
    sapp01.TVFK
  where 
    TRVOG not in ('8')
    and NVL(TVFK._deleted,0) = 0
""")

exclude_proforma.cache()
display(exclude_proforma)
exclude_proforma.createOrReplaceTempView('exclude_proforma')  

# COMMAND ----------

main = spark.sql("""
select 
  distinct
 VBRK.ERNAM AS createdBy
 , Cast(DATE_FORMAT(concat(VBRK.ERDAT,substr(VBRK.ERZET,11,9)),'yyyy-MM-dd HH:mm:ss.SSS') AS TIMESTAMP) AS createdOn 
 , cast(NULL AS STRING) AS modifiedBy
 , VBRK.AEDAT AS modifiedOn
 , CURRENT_TIMESTAMP() AS insertedOn
 , CURRENT_TIMESTAMP() AS updatedOn
 ,TVKO.WAERS  AS baseCurrencyId
 , Concat('BILL_TO-',billTo.KUNNR) AS billToAddressId
--  , Concat('BILL_TO-', VBRK.KUNAG) AS billToAddressId
 , CAST(NULL AS STRING) AS billToCity
 , CAST(NULL AS STRING) AS billToCountry
 , CAST(NULL AS STRING) AS billToLine1
 , CAST(NULL AS STRING) AS billToLine2
 , CAST(NULL AS STRING) AS billToLine3
 , CAST(NULL AS STRING) AS billToPostalCode
 , CAST(NULL AS STRING) AS billToState
 , VBRK.MANDT AS client
 , VBRK.SPART AS customerDivision
 , VBRK.KUNAG AS customerId
 , VBRK.FKDAT AS dateInvoiced
 , VBRK.FKDAT AS dateInvoiceRecognition
 , VBRK.VTWEG AS distributionChannel
--  , NULL AS documentType
 , VBRK.KURRF AS exchangeRate
 , VBRK.FKDAT AS exchangeRateDate
 , CAST(NULL AS STRING) AS exchangeRateType
 , case 
    when VBRK.WAERK = 'USD' then 1
    else EXCHANGE_RATE_AGG.exchangeRate 
  end AS exchangeRateUSD
 , CAST(NULL AS DECIMAL(22,7)) AS exchangeSpotRate
 , CAST(NULL AS DECIMAL(22,7)) AS freightAmount
 , VBRK.VBELN AS invoiceId
 , VBRK.VBELN AS invoiceNumber
 , VBRK.VKORG AS owningBusinessUnitId
 , CAST(NULL AS STRING) AS partyId
--  , Concat('PAY_TO-', payTo.KUNNR) AS PayerAddressId
 , Concat('PAY_TO-', VBRK.KUNAG) AS PayerAddressId
 , CAST(NULL AS STRING) AS salesGroup
 , CAST(NULL AS STRING) AS salesOffice
 , VBRK.VKORG AS salesOrganization
 , CAST(NULL AS STRING) AS salesOrganizationID
 , Concat('SHIP_TO-', shipTo.KUNNR) AS shipToAddressId
 , CAST(NULL AS STRING) AS shipToCity
 , CAST(NULL AS STRING) AS shipToCountry
 , CAST(NULL AS STRING) AS shipToLine1
 , CAST(NULL AS STRING) AS shipToLine2
 , CAST(NULL AS STRING) AS shipToLine3
 , CAST(NULL AS STRING) AS shiptoName
 , CAST(NULL AS STRING) AS shipToPostalCode
 , CAST(NULL AS STRING) AS shipToState
 , Concat('SOLD_TO-', soldTo.KUNNR)  AS soldToAddressId
   --Concat('SOLD_TO-', VBRK.KUNAG)  AS soldToAddressId
 , CAST(NULL AS STRING) AS territoryId
 , CAST(NULL AS decimal(22,7)) AS totalAmount
 , CAST(NULL AS decimal(22,7)) AS totalTax
 , VBRK.WAERK AS transactionCurrencyId
 , VBRK.FKART AS transactionId
FROM VBRK   
  LEFT JOIN BILL_TO_LKP billTo ON VBRK.VBELN = billTo.VBELN and VBRK.MANDT = billTo.MANDT
--   LEFT JOIN PAY_TO_LKP payTo   ON VBRK.VBELN = payTo.VBELN  and VBRK.MANDT =  payTo.MANDT
  LEFT JOIN SHIP_TO_LKP shipTo ON VBRK.VBELN = shipTo.VBELN and VBRK.MANDT = shipTo.MANDT
  LEFT JOIN SOLD_TO_LKP soldTo ON VBRK.VBELN = soldTo.VBELN and VBRK.MANDT = soldTo.MANDT
  join exclude_proforma on vbrk.FKART = exclude_proforma.FKART and vbrk.mandt = exclude_proforma.mandt
  inner join sapp01.tvko on VBRK.mandt = tvko.mandt and VBRK.vkorg = tvko.vkorg
  LEFT OUTER JOIN S_CORE.EXCHANGE_RATE_AGG ON 
      'AVERAGE' || '-' || case when length(trim(VBRK.WAERK)) = 0 then 'EUR' else  trim(VBRK.WAERK) end || '-USD-' || date_format(VBRK.FKDAT, 'yyyy-MM') || '-01-QV' = EXCHANGE_RATE_AGG.fxID
WHERE
  date_format(VBRK.ERDAT, 'yyyyMM') >= '201801'
  and NVL(vbrk._deleted,0) = 0
  and NVL(tvko._deleted,0) = 0
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
  .transform(parse_date(['dateInvoiced','dateInvoiceRecognition'], expected_format = 'MM/dd/yyyy'))
  .transform(parse_timestamp(['createdOn','modifiedOn'], expected_format = 'MM/dd/yyyy')) 
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
            select VBELN AS invoiceId, VKORG AS owningBusinessUnitId
    from
      sapp01.vbrk
    where 
        nvl(vbrk._deleted,0) = 0
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
  cutoff_value = get_incr_col_max_value(salesinvoices,'_MODIFIED')
  update_cutoff_value(cutoff_value, table_name, 'sapp01.VBRK')
  update_run_datetime(run_datetime, table_name, 'sapp01.VBRK')

# COMMAND ----------

# MAGIC %run ../_SHARED/PARTIALS/1.2/footer_s_supplychain
