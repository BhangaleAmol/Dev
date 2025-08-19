# Databricks notebook source
# MAGIC %run ../_SHARED/FUNCTIONS/1.2/bootstrap

# COMMAND ----------

# MAGIC %run ../_SHARED/PARTIALS/1.2/header_s_trademanagement

# COMMAND ----------

# MAGIC %run ../_SCHEMA/s_trademanagement.tradepromotionaccruals

# COMMAND ----------

# LOAD DATASETS
if incremental:
  cutoff_value = get_cutoff_value(table_name, 'ebs.ozf_funds_utilized_all_b', prune_days)
  main_inc = load_incr_dataset('ebs.ozf_funds_utilized_all_b', 'LAST_UPDATE_DATE', cutoff_value)
else:
  main_inc = load_full_dataset('ebs.ozf_funds_utilized_all_b')

# COMMAND ----------

# SAMPLING
if sampling:
  main_inc = main_inc.limit(10)

# COMMAND ----------

# VIEWS
main_inc.createOrReplaceTempView('OZF_FUNDS_UTILIZED_ALL_B')

# COMMAND ----------

CATEGORY_LKP = spark.sql("""
SELECT 
    c.CATEGORY_ID,
    c.ENABLED_FLAG,
    c.PARENT_CATEGORY_ID,
    c.CREATED_BY,
    c.CREATION_DATE,
    c.LAST_UPDATED_BY,
    c.LAST_UPDATE_DATE,
    d.CATEGORY_NAME,
    d.DESCRIPTION
FROM
ebs.AMS_CATEGORIES_B c,
ebs.AMS_CATEGORIES_TL d
WHERE
 c.category_id = d.category_id AND
c.enabled_flag = 'Y' AND
c.CATEGORY_ID like '1000%' AND
d.language = 'US'
""")
CATEGORY_LKP.createOrReplaceTempView("category_lkp")
CATEGORY_LKP.count()

# COMMAND ----------


main = spark.sql("""
                SELECT
                 REPLACE(STRING(INT (ofuab.created_by)), ",", "")						AS createdBy,
                 ofuab.creation_date													AS createdOn,
                 REPLACE(STRING(INT (ofuab.last_updated_by)), ",", "")					AS modifiedBy,
                 ofuab.last_update_date													AS modifiedOn,
                 current_Date()															AS insertedOn,
                 current_Date()															AS updatedOn,
                 REPLACE(STRING(INT (ofuab.cust_account_id)), ",", "")					AS accountId,
                 ofuab.product_level_type												AS accrualLevelType,

                 CASE ofuab.object_type
                   WHEN 'ORDER' 
                   THEN ORDERLINES.actual_shipment_date
                 END AS actualShipDate,

                 ( ofuab.acctd_amount - ofuab.acctd_amount_remaining )					AS amountActual,
                 ofuab.acctd_amount_remaining											AS amountRemaining,
                 hcsua_bill.site_use_code || '-' || REPLACE(STRING(INT (hcsua_bill.site_use_id)), ",", "")			AS billToAddressId,
                 CASE
                   WHEN category_lkp.CATEGORY_ID = '10008'
                   THEN ofuab.acctd_amount_remaining
                   ELSE 0 
                 END AS Bogo,
                 REPLACE(STRING(INT (ofab.category_id)), ",", "")						AS categoryId,
                 CASE
                   WHEN category_lkp.CATEGORY_ID = '10000'
                   THEN ofuab.acctd_amount_remaining
                   ELSE 0 
                 END AS contractRebates,
                 CASE
                   WHEN category_lkp.CATEGORY_ID = '10005'
                   THEN ofuab.acctd_amount_remaining
                   ELSE 0 
                 END AS coopAdvertisingFee,
                 ofuab.currency_code													AS currency,

                 CASE ofuab.object_type
                   WHEN 'ORDER'
                   THEN INVOICEORDER.trx_date
                 END AS dateInvoiced,

                 CASE ofuab.object_type
                   WHEN 'INVOICE'
                   THEN INVOICE.trx_date
                   WHEN 'TP_ORDER'
                   THEN TP_ORDER.date_ordered
                   WHEN 'DM'
                   THEN CMANDDM.trx_date
                   WHEN 'CM'
                   THEN CMANDDM.trx_date
                   WHEN 'ORDER'
                   THEN ORDERHEADER.ordered_date 
                 END  AS documentDate,
                 
                 REPLACE(STRING(INT (
                 CASE ofuab.object_type 
                   WHEN 'INVOICE'
                   THEN INVOICE.trx_number    
                   WHEN 'PCHO'
                   THEN PCHO.segment1
                   WHEN 'DM' 
                   THEN CMANDDM.trx_number
                   WHEN 'CM'
                   THEN CMANDDM.trx_number
                   WHEN 'TP_ORDER'
                   THEN TP_ORDER_HEADER.order_number
                   WHEN 'EXT_ORDER'
                   THEN ' '
                   ELSE ofuab.object_id
                 END 
                 )), ",","") AS documentNumber, 

                 ofuab.object_type														AS documentType,
                 CASE  WHEN ofuab.currency_code = 'USD' THEN 1
                 ELSE  COALESCE(EXCHANGE_RATE_AGG.exchangeRate, 1) END AS exchangeRateUsd,
                 REPLACE(STRING(INT (ofab.fund_id)), ",", "")							AS fundId,
                 ofat.short_name														AS fundName,
                 ofab.fund_number														AS fundNumber,
                 ofab.status_code														AS fundStatusCode,
                 ofab.fund_type															AS fundType,
                 ofuab.gl_date															AS glDate,
                 CASE
                   WHEN category_lkp.CATEGORY_ID = '10002'
                   THEN ofuab.acctd_amount_remaining
                   ELSE 0 
                 END AS gpoAdminFee,
                 REPLACE(STRING(INT (
                 CASE ofuab.object_type
                   WHEN 'ORDER'
                   THEN INVOICEORDER.customer_trx_line_id
                 END 
                 )), ",", "") AS invoiceDetailId,
                 REPLACE(STRING(INT (
                 CASE ofuab.object_type
                   WHEN 'ORDER'
                   THEN INVOICEORDER.customer_trx_id
                 END 
                 )), ",", "") AS invoiceId,
                 CASE ofuab.object_type
                   WHEN 'ORDER'
                   THEN INVOICEORDER.trx_number
                 END As invoiceNumber,
                 REPLACE(STRING(INT (ofuab.product_id)), ",", "")						AS itemId,
                 REPLACE(STRING(INT (
                 CASE ofuab.object_type
                   WHEN 'ORDER'
                   THEN ORDERLINES.line_number
                   WHEN 'TP_ORDER'
                   THEN TP_ORDER.order_line_number
                 END  
                 )), ",", "") AS lineNumber,
                 offer_details.offer_code												AS offerCode, 
                 REPLACE(STRING(INT (ofuab.org_id)), ",", "")							AS owningBusinessUnitId,
                 REPLACE(STRING(INT (ofuab.order_line_id)), ",", "")					AS salesorderDetailId,
                 CASE
                   WHEN category_lkp.CATEGORY_ID = '10007'
                   THEN ofuab.acctd_amount_remaining
                   ELSE 0 
                 END AS setllementDiscount,
                 
                 hcsua_ship.site_use_code || '-' ||REPLACE(STRING(INT (hcsua_ship.site_use_id)), ",","")			AS shipToAddressId,
                 CASE
                   WHEN category_lkp.CATEGORY_ID = '10004'
                   THEN ofuab.acctd_amount_remaining
                   ELSE 0 
                 END AS slottingFee,
                 CASE
                   WHEN category_lkp.CATEGORY_ID = '10003'
                   THEN ofuab.acctd_amount_remaining
                   ELSE 0 
                 END AS tpr,
                 REPLACE(STRING(INT (ofuab.utilization_id)), ",", "")					AS utilizationId,     
                 ofuab.utilization_type													AS utilizationType,
                 CASE
                   WHEN category_lkp.CATEGORY_ID = '10001'
                   THEN ofuab.acctd_amount_remaining
                   ELSE 0 
                 END AS volumeDiscount
              FROM
                  OZF_FUNDS_UTILIZED_ALL_B OFUAB
                  INNER JOIN EBS.OZF_FUNDS_ALL_B OFAB
                    ON ofuab.fund_id = ofab.fund_id
                  INNER JOIN EBS.OZF_FUNDS_ALL_TL OFAT
                    ON ofat.fund_id = ofab.fund_id
                 LEFT JOIN S_CORE.EXCHANGE_RATE_AGG ON  'AVERAGE' || '-' || case when length(trim(ofuab.currency_code)) = 0 then 'USD' else  trim(ofuab.currency_code) end || '-USD-' || date_format(ofuab.gl_date, 'yyyy-MM') || '-01-QV' = EXCHANGE_RATE_AGG.fxID 
                  LEFT JOIN
                          (select distinct
                              a.utilization_id,
                              d.offer_code
                           from ebs.OZF_CLAIM_LINES_UTIL_ALL A
                              LEFT JOIN ebs.ozf_claim_lines_all B 
                                ON a.claim_line_id = b.claim_line_id
                              LEFT Join ebs.ozf_offers D
                                ON b.activity_id = d.qp_list_header_id
                            where d.offer_code is not null
                            ) OFFER_DETAILS
                    ON ofuab.utilization_id = OFFER_DETAILS.utilization_id
                  LEFT JOIN EBS.HZ_CUST_SITE_USES_ALL HCSUA_SHIP
                    ON ofuab.ship_to_site_use_id = hcsua_ship.site_use_id 
                  LEFT JOIN EBS.HZ_CUST_SITE_USES_ALL HCSUA_BILL
                    ON ofuab.bill_to_site_use_id = hcsua_bill.site_use_id 
                  LEFT JOIN (
                            SELECT
                                max(n.trx_number)trx_number, max(n.trx_date)trx_date, n.customer_trx_id
                            FROM ebs.ar_payment_schedules_all n 
                            INNER JOIN ebs.ra_cust_trx_types_all t
                              ON  n.cust_trx_type_id = t.cust_trx_type_id
                            WHERE
                                t.type = 'INV' 
                            GROUP BY n.customer_trx_id) AS  INVOICE
                    ON INVOICE.customer_trx_id = ofuab.object_id 
                  LEFT JOIN(SELECT
                                  Cast(po.segment1 AS VARCHAR(100)), po.po_header_id
                              FROM
                                  ebs.po_headers_all po) PCHO
                    ON PCHO.po_header_id = ofuab.object_id
                  LEFT JOIN (SELECT
                                rct.trx_number, rct.trx_date, rct.customer_trx_id
                            FROM
                                ebs.ra_customer_trx_all rct) CMANDDM
                    ON CMANDDM.customer_trx_id = ofuab.object_id
                  LEFT JOIN(SELECT
                                so.order_number, so.ordered_date, so.header_id
                            FROM
                                ebs.oe_order_headers_all so) ORDERHEADER
                    ON ORDERHEADER.header_id = ofuab.object_id
                  LEFT JOIN(SELECT
                                line_number, actual_shipment_date, line_id
                            FROM
                                ebs.oe_order_lines_all)AS ORDERLINES
                    ON ORDERLINES.line_id = ofuab.order_line_id
                  LEFT JOIN(SELECT
                                order_number, order_line_number, date_ordered, resale_line_id
                            FROM
                                ebs.ozf_resale_lines_all) AS TP_ORDER
                    ON TP_ORDER.resale_line_id = ofuab.object_id
                  LEFT JOIN(SELECT
                                cbheader.order_number, cbline.resale_line_id 
                            FROM
                                ebs.ozf_resale_headers_all cbheader
                                INNER JOIN ebs.ozf_resale_lines_all cbline
                                  ON cbheader.resale_header_id = cbline.resale_header_id
                            ) AS TP_ORDER_HEADER
                    ON TP_ORDER_HEADER.resale_line_id = ofuab.object_id
                  LEFT JOIN(SELECT
                                rct.trx_number, rct.customer_trx_id, null as customer_trx_line_id,--rctl.customer_trx_line_id, --
                                rct.trx_date, rctl.interface_line_attribute6, rctl.org_id
                         
                            FROM
                                ebs.ra_customer_trx_all rct, ebs.ra_customer_trx_lines_all rctl
                            WHERE
                                rctl.customer_trx_id = rct.customer_trx_id
                                AND rctl.line_type = 'LINE'
                                AND rctl.interface_line_context = 'ORDER ENTRY'
                                AND nvl(rctl.DEFERRAL_EXCLUSION_FLAG,'N') = 'N'
                            GROUP BY rct.trx_date, rctl.interface_line_attribute6, rctl.org_id,rct.trx_number, rct.customer_trx_id
                            ) AS INVOICEORDER
                    ON  INVOICEORDER.org_id  = ofuab.org_id
                    AND INVOICEORDER.interface_line_attribute6 = NVL(REPLACE(STRING(INT (ofuab.order_line_id )), ",", ""),CAST(ofuab.order_line_id AS VARCHAR(100)))--CAST(ofuab.order_line_id AS VARCHAR(100))
                  LEFT JOIN category_lkp
                    ON category_lkp.CATEGORY_ID = REPLACE(STRING(INT (ofab.category_id)), ",", "")
              WHERE ofuab.utilization_type <> 'ADJUSTMENT'
                  AND ofat.language = 'US'
""")

main.createOrReplaceTempView('main')
main_2 = main.cache()

# COMMAND ----------

columns = list(schema.keys())
key_columns = ['utilizationId']

# COMMAND ----------

# DROP DUPLICATES
main_3 = remove_duplicate_rows(main_2, key_columns, table_name, 
  source_name, NOTEBOOK_NAME, NOTEBOOK_PATH)

# COMMAND ----------

# TRANSFORM DATA
main_f = (
  main_3
  .transform(tg_default(source_name))
  .transform(tg_trade_management_trade_promotion_accruals())
  .transform(attach_unknown_record)
  .select(columns)
  .transform(apply_schema(schema))
)

# COMMAND ----------

# PERSIST DATA
options = {'target_storage': target_storage, 'target_container': target_container}
merge_to_delta(main_f, table_name, target_folder, overwrite, options = options)

# COMMAND ----------

# HANDLE DELETE
full_keys_f = (
  spark.table('EBS.OZF_FUNDS_UTILIZED_ALL_B')
  .selectExpr('REPLACE(STRING(INT (utilization_id)), ",", "") AS utilizationId')
  .transform(attach_source_column(source = source_name))
  .transform(attach_surrogate_key(columns = [*key_columns, '_SOURCE']))
  .select('_ID')
  .transform(add_unknown_ID())
)

apply_soft_delete(full_keys_f, table_name, key_columns = '_ID')

# COMMAND ----------

# UPDATE FK
update_foreign_key(table_name, 'createdBy,_SOURCE', 'createdBy_ID', 'edm.user')
update_foreign_key(table_name, 'modifiedBy,_SOURCE', 'modifiedBy_ID', 'edm.user')
update_foreign_key(table_name, 'accountId,_SOURCE', 'account_ID', 'edm.account')

# COMMAND ----------

# UPDATE CUTOFF VALUE
if not test_run:  
  cutoff_value = get_incr_col_max_value(main_inc, 'LAST_UPDATE_DATE')
  update_cutoff_value(cutoff_value, table_name, 'ebs.ozf_funds_utilized_all_b')
  update_run_datetime(run_datetime, table_name, 'ebs.ozf_funds_utilized_all_b')

# COMMAND ----------

# MAGIC %run ../_SHARED/PARTIALS/1.2/footer_s_trademanagement
