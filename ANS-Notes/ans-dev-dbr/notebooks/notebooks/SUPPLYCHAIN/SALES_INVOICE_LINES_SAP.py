# Databricks notebook source
# MAGIC %run ../_SHARED/FUNCTIONS/1.2/bootstrap

# COMMAND ----------

# MAGIC %run ../_SHARED/PARTIALS/1.2/header_s_supplychain

# COMMAND ----------

# MAGIC %run ../_SCHEMA/s_supplychain.sales_invoice_lines

# COMMAND ----------

# LOAD DATASETS
if incremental:
  cutoff_value = get_cutoff_value(table_name, 'sapp01.vbrp', prune_days)
  main_inc = load_incr_dataset('sapp01.vbrp', '_MODIFIED', cutoff_value)
else:
  main_inc = load_full_dataset('sapp01.vbrp')

# VIEWS
main_inc.createOrReplaceTempView('main_inc')

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
and nvl(vbpa._deleted,0) = 0
""")
SHIP_TO_LKP.createOrReplaceTempView("SHIP_TO_LKP")


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
SELECT 
  vbrp.ERNAM createdBy,
  Cast(DATE_FORMAT(concat(vbrp.erdat,substr(vbrp.erzet,11,9)),'yyyy-MM-dd HH:mm:ss.SSS') AS TIMESTAMP) AS createdOn,
  CAST(NULL AS STRING) AS modifiedBy,
  CAST(NULL AS TIMESTAMP) AS modifiedOn,
  CURRENT_TIMESTAMP() AS insertedOn,
  CURRENT_TIMESTAMP() AS updatedOn,
  CAST(NULL AS DECIMAL(22,7)) AS accrualBogo,
  CAST(NULL AS DECIMAL(22,7)) AS accrualCoop,
  CAST(NULL AS DECIMAL(22,7)) AS accrualEndUserRebate,
  CAST(NULL AS DECIMAL(22,7)) AS accrualOthers,
  CAST(NULL AS DECIMAL(22,7)) AS accrualTpr,
  CAST(NULL AS DECIMAL(22,7)) AS accrualVolumeDiscount,
  CAST(NULL AS DATE) AS actualDeliveryOn,
  CAST(NULL AS DATE) AS actualShipDate,
  VBRP.MEINS AS ansStdUom,
  case when VBRP.VRKME = VBRP.MEINS then 1
    else vbrp.UMVKZ / vbrp.UMVKN  
  END AS ansStdUomConv,
  case when VBRK.VBTYP = 'N' then vbrp.NETWR * -1 else vbrp.NETWR  end  AS baseAmount,
  CAST(NULL AS DATE) AS bookedDate,
  CAST(NULL AS STRING) AS bookedFlag,
  CAST(NULL AS STRING) AS cancelledFlag,
  CAST(NULL AS DECIMAL(22,7)) AS caseUomConv,
  CAST(NULL AS DATE) AS cetd,
  vbrp.MANDT AS client,
  CAST(NULL AS STRING) AS customerLineNumber,
  CAST(NULL AS STRING) AS customerPoNumber,
  CAST(NULL AS DATE) AS deliverNoteDate,
  CAST(NULL AS STRING) AS deliveryNoteId,
  CAST(NULL AS STRING) AS discountLineFlag,
  CAST(NULL AS STRING) AS distributionChannel,
  vbrp.KURSK AS exchangeRate,
  vbrp.PRSDT AS exchangeRateDate,
  CAST(NULL AS INT) AS intransitTime,
  vbrp.WERKS AS inventoryWarehouseId,
  Concat(vbrp.VBELN , '-' , vbrp.POSNR) AS invoiceDetailId,
  vbrp.VBELN AS invoiceId,
  CAST(NULL AS STRING) AS invoiceUomCode,
  vbrp.MATNR AS itemId,
  case when VBRK.VBTYP = 'N' then  vbrp.WAVWR * -1 else  vbrp.WAVWR  end AS legalEntityCost,
  CAST(NULL AS decimal(22,7)) AS legalEntityCostLeCurrency,
  'LINE' AS lineType,
  CAST(NULL AS STRING) AS lotNumber,
  CAST(NULL AS STRING) AS needByDate,
  CAST(NULL AS decimal(22,7)) AS orderAmount,
  vbrp.MATWA AS orderedItem,
  CAST(NULL AS STRING) AS orderLineHoldType,
  CAST(NULL AS STRING) AS orderLineNumber,
  CAST(NULL AS STRING) AS orderLineStatus,
  AUBEL AS orderNumber,
  CAST(NULL AS STRING) AS orderStatusDetail,
  CAST(NULL AS STRING) ordertypeId,
  vbrp.VRKME AS orderUomCode,
  vbrp.VKORG_AUFT AS owningBusinessUnitId,
  CAST(NULL AS STRING) AS priceListId,
  CAST(NULL AS STRING) AS priceListName,
  vbrp.CMPRE AS pricePerUnit,
  vbrp.MEINS AS primaryUomCode,
  vbrp.UMVKZ/vbrp.UMVKN AS primaryUomConv,
  CAST(NULL AS STRING) AS productCode,
  CAST(NULL AS date) AS promiseDate,
  CAST(NULL AS date) AS promisedOnDate,
  CAST(NULL AS decimal(22,7)) AS quantityBackordered,
  case when VBRK.VBTYP = 'N' then vbrp.FKIMG  else 0 end  AS quantityCancelled,
  case when VBRK.VBTYP = 'N'  then 0 else vbrp.FKIMG end AS quantityInvoiced,
  CAST(NULL AS decimal(22,7)) AS quantityOrdered,
  CAST(NULL AS decimal(22,7)) AS quantityReserved,
  CAST(NULL AS decimal(22,7)) AS quantityReturned,
  CAST(NULL AS decimal(22,7)) AS quantityShipped,
  CAST(NULL AS date) AS requestDeliveryBy,
  CAST(NULL AS date) AS retd,
  CAST(NULL AS decimal(22,7)) AS returnAmount,
  CAST(NULL AS string) AS returnFlag,
  vbrp.AUBEL AS salesOrderId,
  Concat(vbrp.AUBEL , '-' , vbrp.AUPOS) AS salesOrderDetailId,
  CAST(NULL AS date) AS scheduledShipDate,
  CAST(NULL AS date) AS sentDate943,
  CAST(NULL AS int) AS sequenceNumber,
  CAST(NULL AS decimal(22,7)) AS settlementDiscountEarned,
  CAST(NULL AS decimal(22,7)) AS settlementDiscountsCalculated,
  CAST(NULL AS decimal(22,7)) AS settlementDiscountUnEarned,
  CAST(NULL AS date) AS shipDate945,
  case when VBRK.VBTYP = 'N' then  vbrp.WAVWR * -1 else  vbrp.WAVWR  end AS seeThruCost,
  CAST(NULL AS decimal(22,7)) AS seeThruCostLeCurrency,
  Concat('SHIP_TO-',shipto.KUNNR) AS shipToAddressId,
  vbrp.VSTEL AS warehouseCode
FROM
  main_inc vbrp
  join sapp01.vbrk on vbrk.vbeln = vbrp.vbeln and vbrk.mandt = vbrp.mandt 
left  join SHIP_TO_LKP shipto on vbrp.vbeln = shipto.VBELN and vbrp.mandt = shipto.mandt
join exclude_proforma on vbrk.FKART = exclude_proforma.FKART and vbrk.mandt = exclude_proforma.mandt
WHERE
  date_format(vbrp.ERDAT, 'yyyyMM') >= '201801'
  and nvl(vbrp._deleted,0) = 0
  and nvl(vbrk._deleted,0) = 0
  and VBRK.VBTYP in ('M', 'N') 
""")

main.cache()
display(main)

# COMMAND ----------

columns = list(schema.keys())

# COMMAND ----------

file_name = get_file_name(table_name, 'dlt')
target_file = get_file_path(temp_folder, file_name, target_container, target_storage)

main.write.format('delta').mode('overwrite').option('overwriteSchema', 'true').save(target_file)

main_tempDF = spark.read.format('delta').load(target_file)

# COMMAND ----------

main = remove_duplicate_rows(df = main_tempDF, 
                             key_columns = ['invoiceDetailId'], 
                             tableName = table_name, 
                             sourceName = source_name, 
                             notebookName = NOTEBOOK_NAME, 
                             notebookPath = NOTEBOOK_PATH)

# COMMAND ----------

# TRANSFORM DATA
main_f = (
  main
  .withColumn('orderType',f.lit('ORDER_TYPE'))
  .transform(tg_default(source_name))
  .transform(tg_supplychain_sales_invoice_lines())
  .drop('orderType')
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
            select Concat(vbrp.VBELN , '-' , vbrp.POSNR) AS invoiceDetailId 
    from
      sapp01.vbrp
      join sapp01.vbrk on vbrk.vbeln = vbrp.vbeln and vbrk.mandt = vbrp.mandt 
    where 
        nvl(vbrp._deleted,0) = 0
        and nvl(vbrk._deleted,0) = 0
        and VBRK.VBTYP in ('M', 'N')
            """)
  .transform(attach_source_column(source = source_name))
  .transform(attach_surrogate_key(columns = 'invoiceDetailId,_SOURCE'))
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
  cutoff_value = get_incr_col_max_value(main_inc,'_MODIFIED')
  update_cutoff_value(cutoff_value, table_name, 'sapp01.vbrp')
  update_run_datetime(run_datetime, table_name, 'sapp01.vbrp')

# COMMAND ----------

# MAGIC %run ../_SHARED/PARTIALS/1.2/footer_s_supplychain
