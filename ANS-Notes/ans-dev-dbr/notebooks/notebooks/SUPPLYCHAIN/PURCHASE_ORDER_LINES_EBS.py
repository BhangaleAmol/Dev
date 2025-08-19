# Databricks notebook source
# MAGIC %run ../_SHARED/FUNCTIONS/1.2/bootstrap

# COMMAND ----------

# MAGIC %run ../_SHARED/PARTIALS/1.2/header_s_supplychain

# COMMAND ----------

# MAGIC %run ../_SCHEMA/s_supplychain.purchase_order_lines

# COMMAND ----------

# LOAD DATASETS
if incremental:
  cutoff_value = get_cutoff_value(table_name, 'ebs.PO_LINES_ALL', prune_days)
  po_lines_all_inc = load_incr_dataset('ebs.PO_LINES_ALL', 'LAST_UPDATE_DATE', cutoff_value)
else:
  po_lines_all_inc = load_full_dataset('ebs.PO_LINES_ALL')

# COMMAND ----------

# SAMPLING
if sampling:
  po_lines_all_inc = po_lines_all_inc.limit(10)

# COMMAND ----------

# VIEWS
po_lines_all_inc.createOrReplaceTempView('po_lines_all_inc')

# COMMAND ----------

table_exists =hive_table_exists(table_name = 's_supplychain.purchase_order_lines_agg')
if table_exists is True:
    current_cetd_sql="select purchaseOrderDetailId, cetd from s_supplychain.purchase_order_lines_agg where cetd is not null and _source = '" + source_name + "'"
else:
    current_cetd_sql="select '' purchaseOrderDetailId, '' cetd"

current_cetd=spark.sql(current_cetd_sql)
current_cetd.createOrReplaceTempView('current_cetd')

# COMMAND ----------

po_receipts = spark.sql("""
select 
  RCV_TRANSACTIONS.PO_HEADER_ID,
  RCV_TRANSACTIONS.PO_LINE_ID  ,
  sum(NVL(
    RCV_TRANSACTIONS.SOURCE_DOC_QUANTITY,
    RCV_TRANSACTIONS.QUANTITY
  )) AS UNITS_RECEIVED_PURCH_UOM,
  max(RCV_TRANSACTIONS.TRANSACTION_DATE ) lastShipDate 
from
  ebs.RCV_TRANSACTIONS
where 1=1
  AND RCV_TRANSACTIONS.TRANSACTION_TYPE IN (
    'RECEIVE',
    'RETURN TO VENDOR',
    'CORRECT',
    'MATCH'
  )
  and not (
    RCV_TRANSACTIONS.TRANSACTION_TYPE = 'CORRECT'
    and RCV_TRANSACTIONS.DESTINATION_TYPE_CODE = 'INVENTORY'
  )
group by 
  RCV_TRANSACTIONS.PO_HEADER_ID,
  RCV_TRANSACTIONS.PO_LINE_ID 
""")
po_receipts.createOrReplaceTempView('po_receipts')

# COMMAND ----------

GTC_SUPPLIER_MULTI_CETD=spark.sql("""
select 
  ffvv.flex_value vendor_id
from 
  EBS.FND_FLEX_VALUES ffvv
  join EBS.fnd_flex_value_sets ffvs on ffvv.flex_value_set_id = ffvs.flex_value_set_id
where  
  ffvs.flex_value_set_name = 'XXANS_GTC_SUPPLIER_MULTI_CETD'
""")
GTC_SUPPLIER_MULTI_CETD.createOrReplaceTempView('GTC_SUPPLIER_MULTI_CETD')

# COMMAND ----------

check_val_emea_po=spark.sql("""
SELECT
      distinct pha.segment1,
      'Y' flag
    FROM
      ebs.oe_drop_ship_sources ods,
      ebs.oe_order_headers_all ooh,
      ebs.oe_order_lines_all ool,
      ebs.hz_cust_accounts hca,
      ebs.hz_parties hp,
      ebs.fnd_lookup_values flv,
      ebs.po_headers_all pha
    WHERE
      pha.org_id = 938
      AND ods.po_header_id = pha.po_header_id
      AND ooh.header_id = ods.header_id
      AND ooh.header_id = ool.header_id
      AND ooh.sold_to_org_id = hca.cust_account_id
      AND hp.party_id = hca.party_id
      AND hca.account_number = flv.lookup_code
      AND flv.lookup_type = 'XXANS_EMEA_CUSTOMER'
      AND flv.language = 'US'
      AND flv.enabled_flag = 'Y'
""")
check_val_emea_po.createOrReplaceTempView('check_val_emea_po')

# COMMAND ----------

check_pto_dropship_so=spark.sql("""
SELECT
      distinct pha.segment1,
      'Y' flag
    FROM
      ebs.oe_order_headers_all ooh,
      ebs.oe_transaction_types_tl ott,
      ebs.oe_drop_ship_sources ods,
      ebs.po_headers_all pha
    WHERE
      1 = 1
      AND pha.org_id = 938 --AND pha.segment1 = '15191806'
      AND ott.transaction_type_id = ooh.order_type_id
      AND ooh.header_id = ods.header_id
      AND ods.po_header_id = pha.po_header_id
      AND UPPER (ott.name) IN (SELECT UPPER (meaning)
        FROM
          ebs.fnd_lookup_values
        WHERE
          enabled_flag = 'Y'
          AND current_date BETWEEN NVL (start_date_active,current_date) AND NVL (end_date_active,current_date)
          AND TRIM (UPPER (lookup_type)) = 'XXASL_ROA_DROPSHIP_ORDER_TYPES')
""")
check_pto_dropship_so.createOrReplaceTempView('check_pto_dropship_so')

# COMMAND ----------

rsh = spark.sql("""
 select
      poh.segment1,
      max(rsh.EXPECTED_RECEIPT_DATE) EXPECTED_RECEIPT_DATE
    from
      ebs.rcv_shipment_headers rsh,
      ebs.rcv_shipment_lines rsl,
      ebs.po_headers_all poh,
      ebs.po_lines_all pla
    where
      rsh.shipment_header_id = rsl.shipment_header_id
      and rsl.po_header_id = poh.po_header_id
      and rsl.PO_LINE_ID = pla.PO_LINE_ID
      and rsl.shipment_line_status_code = 'EXPECTED'
    group by
      poh.segment1
""")
rsh.createOrReplaceTempView('rsh')

# COMMAND ----------

main = spark.sql("""
with q1 as
(
 select DISTINCT
  REPLACE(STRING(INT(POL.CREATED_BY)), ",", "") AS createdBy,
  POL.CREATION_DATE createdOn,
  REPLACE(STRING(INT(POL.LAST_UPDATED_BY)), ",", "") AS modifiedBy,
  POL.LAST_UPDATE_DATE modifiedOn,
  CURRENT_DATE insertedOn,
  CURRENT_DATE updatedOn,
  nvl(POL.CANCEL_FLAG, 'N') cancelledFlag,
  current_cetd.cetd current_cetd,
  max(
    CASE 
      -- Existing order follow buffer calculation
      WHEN (date_format (poh.creation_date, 'yyyyMMdd') BETWEEN '20220520' AND '20221216' 
        OR date_format (pla.last_update_date, 'yyyyMMdd') BETWEEN '20220520' AND  '20221216' )
        and nvl(check_val_emea_po.flag,'N') = 'N'
        and NVL(check_pto_dropship_so.flag,'N') = 'N' 
        THEN DATE_ADD(PLA.PROMISED_DATE, - INT(POL.IN_TRANSIT_TIME) -21) -- (SELECT apps.fnd_profile.value@OBI_ANSPRD('XX_ANSELL_PO_CETD_UPDATE_DAYS') FROM DUAL)
      -- Previously buffer for 14 days created before 19-May-2022
      WHEN (date_format (poh.creation_date, 'yyyyMMdd') BETWEEN '20200101' AND '20220519'
        OR date_format (pla.last_update_date, 'yyyyMMdd') BETWEEN '20200101'  AND '20220519')
        and NVL(check_val_emea_po.flag,'N') = 'N'
        and NVL(check_pto_dropship_so.flag,'N') = 'N' THEN DATE_ADD(PLA.PROMISED_DATE, - INT(POL.IN_TRANSIT_TIME) -14)
      --SAP order no buffer for single CETD
      WHEN (NVL(check_val_emea_po.flag,'N') = 'Y')
        and GTC_SUPPLIER_MULTI_CETD.vendor_id is null THEN DATE_ADD(PLA.PROMISED_DATE, - INT(POL.IN_TRANSIT_TIME))
      -- SAP order for MCETD 
      WHEN NVL(check_val_emea_po.flag,'N') = 'Y'
        and GTC_SUPPLIER_MULTI_CETD.vendor_id is not null  THEN DATE_ADD(rsh.EXPECTED_RECEIPT_DATE, - INT(POL.IN_TRANSIT_TIME)
      )
      WHEN (
        date_format (poh.creation_date, 'yyyyMMdd') BETWEEN '20220520'
        AND date_format(current_date, 'yyyyMMdd')
        OR date_format (pla.last_update_date, 'yyyyMMdd') BETWEEN '20220520'
        AND date_format(current_date, 'yyyyMMdd')
      )
      and (NVL(check_pto_dropship_so.flag,'N') = 'Y') THEN DATE_ADD(PLA.PROMISED_DATE, - INT(POL.IN_TRANSIT_TIME) -21) -- (SELECT apps.fnd_profile.value@OBI_ANSPRD('XX_ANSELL_PTO_DS_CETD_UPDATE_DAYS') FROM DUAL)
      WHEN (
        date_format (poh.creation_date, 'yyyyMMdd') BETWEEN '20200101'
        AND '20220519'
        OR date_format (pla.last_update_date, 'yyyyMMdd') BETWEEN '20200101'
        AND '20220519'
      )
      and (NVL(check_pto_dropship_so.flag,'N') = 'Y') THEN DATE_ADD(PLA.PROMISED_DATE, - INT(POL.IN_TRANSIT_TIME))
      ELSE DATE_ADD(PLA.PROMISED_DATE, - INT(POL.IN_TRANSIT_TIME))
    END
  ) CETD,
  POL.CLOSED_DATE closedDate,
  POH.IN_TRANSIT_TIME inTransitTime,
  NULL AS inventoryWarehouseID,
  REPLACE(STRING(INT(POL.ITEM_ID)), ",", "") AS itemId,
  POL.LINE_NUM lineNumber,
  max(PLA.NEED_BY_DATE) needByDate,
  POL.CLOSED_CODE orderLineStatus,
  POL.UNIT_MEAS_LOOKUP_CODE orderUomCode,
  POL.UNIT_PRICE pricePerUnit,
  REPLACE(STRING(INT(POL.PO_LINE_ID)), ",", "") AS purchaseOrderDetailId,
  REPLACE(STRING(INT(POL.PO_HEADER_ID)), ",", "") AS purchaseOrderId,
  nvl(po_receipts.UNITS_RECEIVED_PURCH_UOM,0) AS quantityDelivered,
  REPLACE(STRING(INT(POL.QUANTITY)), ",", "") quantityOrdered,
  nvl(po_receipts.UNITS_RECEIVED_PURCH_UOM,0) AS quantityShipped,
  max(DATE_ADD(pla.need_by_date, - INT(POL.IN_TRANSIT_TIME))) RETD,
  po_receipts.lastShipDate AS  shippedDate
from 
  po_lines_all_inc POL 
  join ebs.PO_HEADERS_ALL POH 
    on POL.PO_HEADER_ID = POH.PO_HEADER_ID
  join EBS.MTL_SYSTEM_ITEMS_B MSIB ON POL.ITEM_ID = MSIB.Inventory_item_id
  join ebs.po_line_locations_all pla on POL.PO_LINE_ID = PLA.PO_LINE_ID  
  left join GTC_SUPPLIER_MULTI_CETD on POH.vendor_id = GTC_SUPPLIER_MULTI_CETD.vendor_id
  left join check_pto_dropship_so on poh.segment1 = check_pto_dropship_so.segment1
  left join check_val_emea_po on poh.segment1 = check_val_emea_po.segment1
  left join po_receipts 
    on POL.PO_HEADER_ID = po_receipts.PO_HEADER_ID
    AND POL.PO_LINE_ID = po_receipts.PO_LINE_ID 
  left join current_cetd on REPLACE(STRING(INT(POL.PO_LINE_ID)), ",", "") = current_cetd.purchaseOrderDetailId
  left join rsh on poh.segment1 = rsh.segment1
where 1=1
  AND MSIB.ORGANIZATION_ID = 124
  AND NVL (pla.cancel_flag, 'N') <> 'Y'
  AND poh.type_lookup_code = 'STANDARD'
  and date_format(poh.creation_date, 'yyyyMMdd') >= '20180701'
  and POL.item_id is not null
  AND poh.org_id IN (select distinct key_value from smartsheets.edm_control_table where table_id = 'WC_QV_PO_DATES')
GROUP BY
  REPLACE(STRING(INT(POL.CREATED_BY)), ",", ""),
  POL.CREATION_DATE,
  REPLACE(STRING(INT(POL.LAST_UPDATED_BY)), ",", "") ,
  POL.LAST_UPDATE_DATE ,
  nvl(POL.CANCEL_FLAG, 'N') ,
  current_cetd.cetd,
  POL.CLOSED_DATE ,
  POH.IN_TRANSIT_TIME ,
  REPLACE(STRING(INT(POL.ITEM_ID)), ",", "") ,
  POL.LINE_NUM ,
  POL.CLOSED_CODE ,
  POL.UNIT_MEAS_LOOKUP_CODE ,
  POL.UNIT_PRICE ,
  REPLACE(STRING(INT(POL.PO_LINE_ID)), ",", "") ,
  REPLACE(STRING(INT(POL.PO_HEADER_ID)), ",", "") ,
  nvl(po_receipts.UNITS_RECEIVED_PURCH_UOM,0) ,
  REPLACE(STRING(INT(POL.QUANTITY)), ",", "") ,
  nvl(po_receipts.UNITS_RECEIVED_PURCH_UOM,0) ,
  po_receipts.lastShipDate 
)
select 
  q1.createdBy,
  q1.createdOn,
  q1.modifiedBy,
  q1.modifiedOn,
  insertedOn,
  CURRENT_DATE updatedOn,
  q1.cancelledFlag,
  nvl(q1.current_cetd, q1.cetd) cetd,
  q1.closedDate, 
  q1.inTransitTime,
  q1.inventoryWarehouseID,
  q1.itemId,
  q1.lineNumber,
  q1.needByDate,
  q1.orderLineStatus,
  q1.orderUomCode,
  q1.pricePerUnit,
  q1.purchaseOrderDetailId,
  q1.purchaseOrderId,
  q1.quantityDelivered,
  q1.quantityOrdered,
  q1.quantityShipped,
  q1.cetd rcetd,
  q1.retd retd,
  q1.shippedDate
from 
  q1
""")
main.cache()

# display(main)

# COMMAND ----------

columns = list(schema.keys())

# COMMAND ----------

main = remove_duplicate_rows(df = main, 
                             key_columns = ['purchaseOrderDetailId'], 
                             tableName = table_name, 
                             sourceName = source_name, 
                             notebookName = NOTEBOOK_NAME, 
                             notebookPath = NOTEBOOK_PATH)

# COMMAND ----------

# TRANSFORM DATA
main_f = (
  main
  .transform(tg_default(source_name))
  .transform(tg_supplychain_purchaseorderlines())
  .transform(attach_surrogate_key(columns = 'purchaseOrderDetailId,_SOURCE'))
  .transform(apply_schema(schema))
  .transform(attach_unknown_record)
  .select(columns)
  .transform(sort_columns)
)

main_f.cache()
main_f.createOrReplaceTempView('main_f')
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
        REPLACE(STRING(INT(PO_LINE_ID)), ",", "") AS purchaseOrderDetailId
    FROM ebs.PO_LINES_ALL
   """)
  .transform(attach_source_column(source = source_name))
  .transform(attach_surrogate_key(columns = 'purchaseOrderDetailId,_SOURCE'))
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
  cutoff_value = get_incr_col_max_value(po_lines_all_inc)
  update_cutoff_value(cutoff_value, table_name, 'ebs.PO_LINES_ALL')
  update_run_datetime(run_datetime, table_name, 'ebs.PO_LINES_ALL')

# COMMAND ----------

# MAGIC %run ../_SHARED/PARTIALS/1.2/footer_s_supplychain
