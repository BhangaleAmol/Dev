# Databricks notebook source
# MAGIC %run ../_SHARED/FUNCTIONS/1.2/bootstrap

# COMMAND ----------

# MAGIC %run ../_SHARED/PARTIALS/1.2/header_s_supplychain

# COMMAND ----------

# MAGIC %run ../_SCHEMA/s_supplychain.sales_order_headers

# COMMAND ----------

# LOAD DATASETS
vw_tembo_orders = load_full_dataset('col.vw_tembo_orders')
vw_tembo_orders.createOrReplaceTempView('vw_tembo_orders')

# COMMAND ----------

# SAMPLING
if sampling:
  vw_tembo_orders = vw_tembo_orders.limit(10)
  vw_tembo_orders.createOrReplaceTempView('vw_tembo_orders')

# COMMAND ----------

# CHANGE GRAIN
vw_tembo_orders_agg = (
  vw_tembo_orders
  .groupBy('Currency', 'CustomerID', 'Customer_PO', 'Company', 'Order_Status', 'Order_Number', 'ShipToDeliveryLocationID', 'DOC_Currency')
  .agg(
    f.max("Ordered_Date").alias("Ordered_Date"),
    f.max("Request_Date").alias("Request_Date")
  )
)

vw_tembo_orders_agg.createOrReplaceTempView('vw_tembo_orders_agg')
display(vw_tembo_orders_agg)

# COMMAND ----------

main = spark.sql("""
SELECT
  NULL AS createdBy,
  NULL AS createdOn,
  NULL AS modifiedBy,
  NULL AS modifiedOn,
  CURRENT_TIMESTAMP() AS insertedOn,
  CURRENT_TIMESTAMP() AS updatedOn,
  o.Currency AS baseCurrencyId,
  Concat('BILL_TO-',o.CustomerID) AS billToAddressId,
  NULL AS client,
  NULL AS csr,
  NULL AS customerDivision,
  o.company || '-' || o.CustomerID AS customerId,
  o.Customer_PO AS customerPoNumber,
  NULL AS customerServiceRepresentative,
  NULL AS distributionChannel,
  NULL AS division,
  NULL AS dropShipPoNumber,
  NULL AS eDIAutoBooking,
  NULL AS eDIAutoBookingSuccess,
  CASE
    WHEN o.Currency = DOC_Currency THEN 1
  END AS exchangeRate,
  NULL AS exchangeRateDate,
  NULL AS exchangeRateType,
  EXCHANGE_RATE_AGG.exchangeRate exchangeRateUsd,
  NULL AS exchangeSpotRate,
  NULL AS freightTerms,
  NULL AS gbu,
  o.Company AS legalEntityID,
  NULL AS openOrderFlag,
  TO_DATE(o.Ordered_Date, 'yyyy-MM-dd HH:mm:ss') AS orderDate,
  TO_DATE(o.Ordered_Date, 'yyyy-MM-dd HH:mm:ss') AS orderDateTime,
  NULL as orderEntryType, 
  o.Order_Status AS orderHeaderStatus,
  NULL AS orderHoldDate1,
  NULL AS orderHoldDate2,
  NULL AS orderHoldBy1Id,
  NULL AS orderHoldBy2Id,
  NULL AS orderHoldType,
  o.Order_number AS orderNumber,
  NULL AS orderReason,
  'Standard' AS ordertypeId,
  NULL AS orderVolume,
  NULL AS orderWeight,
  o.Company AS owningBusinessUnitId,
  NULL AS partyId,
  Concat('PAY_TO-',o.CustomerID)AS PayerAddressId,
  NULL paymentMethodId,
  NULL paymentTermId,
  NULL AS regionalSalesManager,
  TO_DATE(o.Request_Date, 'yyyy-MM-dd HH:mm:ss') AS requestDeliveryBy,
  NULL AS returnReasonCode,
  NULL AS robomotionNumberOfSubscriptions,
  NULL AS robomotionSubscription,
  NULL AS salesGroup,
  NULL AS salesOffice,
  o.Order_Number AS salesOrderId,
  NULL AS salesOrganization,
  NULL AS salesOrganizationID,
  NULL AS shipmentPriority,
  Concat('SHIP_TO-',o.ShipToDeliveryLocationID) AS shipToAddressId,
  Concat('SOLD_TO-',o.CustomerID) as soldToAddressid,
  NULL AS sourceOrderReference,
  NULL AS territoryId,
  NULL AS territorySalesManager,
  o.DOC_Currency AS transactionCurrencyId
FROM
  vw_tembo_orders_agg o
 LEFT OUTER JOIN S_CORE.EXCHANGE_RATE_AGG ON 
      'AVERAGE' || '-' || case when length(trim(o.DOC_Currency)) = 0 then 'COL' else  trim(o.DOC_Currency) end || '-USD-' || date_format(o.Ordered_Date, 'yyyy-MM') || '-01-QV' = EXCHANGE_RATE_AGG.fxID
""")

main.cache()
display(main)

# COMMAND ----------

columns = list(schema.keys())

# COMMAND ----------

main = remove_duplicate_rows(df = main, 
                             key_columns = ['salesOrderId'], 
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
  .transform(tg_supplychain_sales_order_headers())
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
  spark.table('col.vw_tembo_orders')
  .filter('_DELETED IS FALSE')
  .selectExpr("Order_Number AS salesOrderId")
  .select('salesOrderId')
  .transform(attach_source_column(source = source_name))
  .transform(attach_surrogate_key(columns = 'salesOrderId,_SOURCE'))
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
  cutoff_value = get_incr_col_max_value(vw_tembo_orders)
  update_cutoff_value(cutoff_value, table_name, 'col.vw_tembo_orders')
  update_run_datetime(run_datetime, table_name, 'col.vw_tembo_orders')

# COMMAND ----------

# MAGIC %run ../_SHARED/PARTIALS/1.2/footer_s_supplychain
