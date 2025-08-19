# Databricks notebook source
# MAGIC %run ../_SHARED/FUNCTIONS/1.2/bootstrap

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW g_fin_qv.wc_qv_order_data_full_v AS
# MAGIC  select
# MAGIC    KEY,
# MAGIC    Company_Number_Detail,
# MAGIC    PROD_NUM,
# MAGIC    ORDER_NUM,
# MAGIC    ORDER_LINE_NUM,
# MAGIC    ORDER_LINE_DETAIL_NUM,
# MAGIC    ORDER_DT,
# MAGIC    REQUEST_DT,
# MAGIC    SCHEDULE_DT,
# MAGIC    SHIP_DT,
# MAGIC    ORDER_STATUS,
# MAGIC    ORDER_QTY_STD_UOM,
# MAGIC    ORDERED_AMOUNT_DOC_CURR,
# MAGIC    LE_CURRENCY,
# MAGIC    CUSTUMER_ID,
# MAGIC    SHIP_TO_DELIVERY_LOCATION_ID,
# MAGIC    ORDER_TYPE,
# MAGIC    CUST_PO_NUM,
# MAGIC    SOURCE_ORDER_STATUS,
# MAGIC    ORGANIZATION_CODE,
# MAGIC    W_UPDATE_DT,
# MAGIC    DOC_CURR_CODE,
# MAGIC    DISTRIBUTOR_ID,
# MAGIC    DISTRIBUTOR_NAME,
# MAGIC    X_DELIVERY_NOTE_ID,
# MAGIC    X_ORDER_HOLD_TYPE,
# MAGIC    X_RESERVATION_VALUE,
# MAGIC    X_INTRANSIT_TIME,
# MAGIC    X_SHIPDATE_945,
# MAGIC    X_FREIGHT_TERMS,
# MAGIC    X_CREATION_DATE,
# MAGIC    X_BOOKED_DATE,
# MAGIC    X_SHIPPED_QUANTITY,
# MAGIC    X_SHIPPING_QUANTITY_UOM,
# MAGIC    X_NEED_BY_DATE,
# MAGIC    X_RETD,
# MAGIC    X_PROMISED_DATE,
# MAGIC    X_CETD,
# MAGIC    ORDER_DATE_TIME,
# MAGIC    SO_PROMISED_DATE,
# MAGIC    CANCELLED_QTY_STD_UOM,
# MAGIC    X_RESERVATION_QTY,
# MAGIC    ORDERED_ITEM,
# MAGIC    DELIVERY_NOTE_DATE,
# MAGIC    CUSTOMER_LINE_NUMBER,
# MAGIC    SOURCE_NAME,
# MAGIC    RM_NUM_OF_SUBSCRIPTIONS,
# MAGIC    RM_SUBSCRIPTION
# MAGIC  from
# MAGIC    g_fin_qv.wc_qv_order_data_full

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW g_fin_qv.wc_qv_order_data_v AS
# MAGIC select
# MAGIC   KEY,
# MAGIC   Company_Number_Detail,
# MAGIC   PROD_NUM,
# MAGIC   ORDER_NUM,
# MAGIC   ORDER_LINE_NUM,
# MAGIC   ORDER_LINE_DETAIL_NUM,
# MAGIC   ORDER_DT,
# MAGIC   REQUEST_DT,
# MAGIC   SCHEDULE_DT,
# MAGIC   SHIP_DT,
# MAGIC   ORDER_STATUS,
# MAGIC   ORDER_QTY_STD_UOM,
# MAGIC   ORDERED_AMOUNT_DOC_CURR,
# MAGIC   LE_CURRENCY,
# MAGIC   CUSTUMER_ID,
# MAGIC   SHIP_TO_DELIVERY_LOCATION_ID,
# MAGIC   ORDER_TYPE,
# MAGIC   CUST_PO_NUM,
# MAGIC   SOURCE_ORDER_STATUS,
# MAGIC   ORGANIZATION_CODE,
# MAGIC   W_UPDATE_DT,
# MAGIC   DOC_CURR_CODE,
# MAGIC   DISTRIBUTOR_ID,
# MAGIC   DISTRIBUTOR_NAME,
# MAGIC   X_DELIVERY_NOTE_ID,
# MAGIC   X_ORDER_HOLD_TYPE,
# MAGIC   X_RESERVATION_VALUE,
# MAGIC   X_INTRANSIT_TIME,
# MAGIC   X_SHIPDATE_945,
# MAGIC   X_FREIGHT_TERMS,
# MAGIC   X_CREATION_DATE,
# MAGIC   X_BOOKED_DATE,
# MAGIC   X_SHIPPED_QUANTITY,
# MAGIC   X_SHIPPING_QUANTITY_UOM,
# MAGIC   X_NEED_BY_DATE,
# MAGIC   X_RETD,
# MAGIC   X_PROMISED_DATE,
# MAGIC   X_CETD,
# MAGIC   ORDER_DATE_TIME,
# MAGIC   SO_PROMISED_DATE,
# MAGIC   CANCELLED_QTY_STD_UOM,
# MAGIC   X_RESERVATION_QTY,
# MAGIC   ORDERED_ITEM,
# MAGIC   DELIVERY_NOTE_DATE,
# MAGIC   CUSTOMER_LINE_NUMBER,
# MAGIC   SOURCE_NAME,
# MAGIC   RM_NUM_OF_SUBSCRIPTIONS,
# MAGIC   RM_SUBSCRIPTION
# MAGIC from
# MAGIC   g_fin_qv.wc_qv_order_data

# COMMAND ----------

# %sql
# CREATE OR REPLACE VIEW g_fin_qv.wc_qv_global_inv_ord_a AS
# select
#    oh.customerPONumber CUST_PO_NUM,
#    nvl(st.addressLine1,'') SHIP_TO_NAME,
#    nvl(st.addressLine2, '') ADDRESS_1,
#    nvl(st.addressLine3, '') ADDRESS_2,
#    nvl(st.addressLine4, '') ADDRESS_3,
#    nvl(st.state, '') AS STATE,
#    nvl(st.postalCode,'') AS ZIP_CODE,
#    inv.organizationCode AS COMP_NUM,
#    oh.orderNumber AS ORDER_NUM,
#    ol.lineNumber AS ORDER_LINE_NUM,
#    ol.sequenceNumber AS ORDER_LINE_DETAIL_NUM,
#    nvl(st.accountNumber, '') AS CUST_NUM,
#    pr.productCode AS PROD_NUM,
#    pr.name AS PROD_NAME,
#    ol.quantityordered * ol.ansStdUomConv AS ORDER_QTY_STD_UOM,  
#   round(ol.quantityordered * ol.primaryUomConv,0) ORDER_QTY_PRIM_UOM,
#   ol.quantityshipped * ol.ansStdUomConv SHIP_QTY_STD_UOM,
#   case
#     when
#       quantityordered =  quantityshipped
#         then 'Y'
#      else 'N'
#     end  ORDER_COMPLETED,
#     case when ol.orderLineHoldType is not null
#       or oh.orderHoldType is not null
#       then 'On Hold'
#      else
#       ol.orderStatusDetail
#      end AS ORDER_STATUS,
#     oh.baseCurrencyId AS LE_CURR,
#     oh.transactionCurrencyId AS DOC_CURR,
#     ol.orderUomCode AS ORDER_UOM,
#     pr.ansStdUom AS STD_UOM,
#     oh.orderDate AS ORDER_DT,
#     oh.requestDeliveryBy AS REQUEST_DT,
#     ol.actualShipDate AS SHIP_DT,
#     ol.orderAmount/(ol.quantityordered * ol.ansStdUomConv) AS UNIT_PRICE_STD_UOM_DOC,
#     (ol.orderAmount/(ol.quantityordered * ol.ansStdUomConv))/oh.exchangeRateUsd AS UNIT_PRICE_STD_UOM_USD,
#     (ol.orderAmount/(ol.quantityordered * ol.ansStdUomConv))*oh.exchangeRate AS UNIT_PRICE_STD_UOM_LE,
#     inv.organizationCode AS WAREHOUSE,
#     ol.ansStdUomConv CONV_FACTOR,
#     0 AS ORIG_CUST_NUM,
#     '' AS END_CUST_PO,
#     0 AS END_CUST,
#     pr.productDivision AS ACCT_DIV,
#     CASE
#         WHEN
#           tt.name like '%Direct Shipment%' OR tt.name LIKE '%Drop Shipment%'
#         THEN 'DS'
#       ELSE
#         'WH'
#     END DROP_SHIPMARK_CUST_ORD,
#     ol.cetd AS CETD,
#     ol.cetd AS REV_CETD,
#     '' AS CUST_PROD,
#     ol.pricePerUnit AS LIST_PRICE_DOC_CURR,
#     CASE
#           WHEN tt.name LIKE '%Direct Shipment%' THEN 'Direct Shipment'
#           WHEN tt.name LIKE '%Consignment%' THEN 'Consignment'
#           WHEN tt.name LIKE '%Credit Memo%' THEN 'Credit Memo'
#           WHEN tt.name LIKE '%Drop Shipment%' THEN 'Drop Shipment'
#           WHEN tt.name LIKE '%Free Of Charge%' THEN 'Free Of Charge'
#           WHEN tt.name LIKE '%GTO%' THEN 'GTO'
#           WHEN tt.name LIKE '%Intercompany%' THEN 'Intercompany'
#           WHEN tt.name LIKE '%Replacement%' THEN 'Replacement'
#           WHEN tt.name LIKE '%Sample Return%' THEN 'Sample Return'
#           WHEN tt.name LIKE '%Return%' THEN 'Return'
#           WHEN tt.name LIKE '%Sample%' THEN 'Sample'
#           WHEN tt.name LIKE '%Service%' THEN 'Service'
#           WHEN tt.name LIKE '%Standard%' THEN 'Standard'
#           WHEN tt.name LIKE '%Customer_Safety_Stock%' THEN 'Safety Stock'
#          ELSE tt.name
#     END ORDER_TYPE,
#     ol.orderAmount As LINE_VALUE_DOC_CURR,
#     (ol.orderAmount/oh.exchangeRateUsd) AS LINE_VALUE_USD,
#     0 AS LINE_COST_LE_CURR,
#     pr.productBrand As BRAND,
#     pr.productSubBrand AS SUBBRAND--,
#   --  inv.organizationCode,
#   --  ol.bookedFlag
# from
#   s_supplychain.sales_order_headers_agg oh
#   join s_supplychain.sales_order_lines_agg ol on oh._ID = ol.salesOrder_ID
#   join s_core.organization_agg org on oh.owningBusinessUnit_ID = org._ID
#   join s_core.organization_agg inv on ol.inventoryWarehouse_ID = inv._ID
#   join s_core.product_agg pr on ol.item_ID = pr._ID
# --   join s_core.account_agg ac on oh.customer_ID = ac._ID
#   join s_core.customer_location_agg st on ol.shipToAddress_ID = st._ID
#   join s_core.transaction_type_agg tt on oh.orderType_ID = tt._ID
# where 1=1
#     AND tt.name NOT LIKE '%Safety Stock%'
#     AND tt.name NOT LIKE '%Return%'
#     AND tt.name NOT LIKE '%Sample%'
#     AND tt.name NOT LIKE '%Consignment%'
#     AND tt.name NOT LIKE '%Credit Memo%'
#     AND tt.name NOT LIKE '%Free Of Charge%'
#     AND tt.name NOT LIKE '%Replacement%'
#     AND tt.name NOT LIKE '%Sample Return%'
#     AND tt.name NOT LIKE '%Service%'
#     AND ol.quantityordered !=0
#     AND inv.organizationCode IN ('508', '509', '511','532','800','805','325','811','832','600','601','605','401','802','819','602','724', '826', '827')
#     AND ol.orderStatusDetail !='Cancelled'
#     AND date_format(oh.orderDate, 'yyyyMM') between date_format(add_months(current_date, -11), 'yyyyMM') and date_format(current_date, 'yyyyMM')
#     and ol.bookedFlag = 'Y'
#     and not ol._deleted
#     and not oh._deleted

# COMMAND ----------

# %sql
# CREATE OR REPLACE VIEW g_fin_qv.customer_shipto AS
# select
#   distinct loc.partySiteNumber as ShipToAddressID,
#   acc.accountNumber as customerID,
#   acc.accountNumber || '-' || loc.partySiteNumber as CustShipto,
#   acc.name as CustomerName,
#   loc.addressLine1 as AddressLine1,
#   loc.addressLine2 as AddressLine2,
#   loc.city as City,
#   loc.postalCode as PostalCode,
#   loc.stateName as StateProvince,
#   terr.territoryShortName as Country,
#   CASE
#     WHEN loc.siteCategory = 'LA' THEN 'LA'
#     ELSE 'NA'
#   END as Region,
#   acc.customerDivision as Division,
#   acc.industry as CustomerIndustry,
#   acc.subIndustry as CustomerSubIndustry,
#   acc.vertical AS Vertical
# from
#   s_core.customer_location_ebs loc,
#   s_core.account_ebs acc,
#   s_core.territory_ebs terr
# where
#   acc._id = loc.account_id
#   and loc.territory_ID = terr._id
#   and acc.customerType = 'External'
# order by
#   2

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace view g_fin_qv.wc_qv_gross_cbo_v as
# MAGIC select
# MAGIC   SOURCE,
# MAGIC   MONTH_END_DATE,
# MAGIC   ITEM_NUMBER,
# MAGIC   ITEM_DESCRIPTION,
# MAGIC   INVENTORY_ORG_CODE,
# MAGIC   INVENTORY_ORG_NAME,
# MAGIC   OPERATING_UNIT,
# MAGIC   STD_UOM,
# MAGIC   PRIMARY_UOM,
# MAGIC   CBO_QTY,
# MAGIC   CBO_QTY_PRIMARY_UOM,
# MAGIC   CBO_VALUE_USD,
# MAGIC   ORDER_TYPE,
# MAGIC   DIRECT_SHIP_FLG,
# MAGIC   SHIP_TO_REGION,
# MAGIC   SALES_ORDER,
# MAGIC   ORDERED_DATE,
# MAGIC   REQUEST_DATE,
# MAGIC   SCHEDULED_DATE,
# MAGIC   CUSTOMER_NUMBER,
# MAGIC   CUSTOMER_NAME,
# MAGIC   STATUS_NAME,
# MAGIC   ORIGIN_CODE ,
# MAGIC   ORIGIN_DESCRIPTION ,
# MAGIC   XACT_SUBTYPE_CODE
# MAGIC from
# MAGIC   g_fin_qv.wc_qv_gross_cbo

# COMMAND ----------

# %sql
# create or replace view g_fin_qv.wc_pwbi_ap_invoice_a as
# SELECT
# ap.purchaseinvoicenumber as INVOICE_NUM,
# ap.invoiceId as INVOICE_ID,
# ap.owningBusinessUnitId as ORG_ID,
# date_format(ap.postdate,'MMM-yy') as PERIOD_NAME,
# ap.postDate as INV_GL_DATE,
# sup.supplierNumber as VENDOR_NUM,
# sup.supplierName as VENDOR_NAME,
# null INVOICE_LINE_NUMBER,
# ap.purchaseInvoiceLine LINE_NUMBER,
# SUM(ap.documentAmount*-1 ) as INV_DIST_AMT,
# ap.reversalFlag as AP_ADJ,
# modified.login as INV_LAST_UPDATED_BY,
# created.login as INV_CREATED_BY,
# ap.dateInvoiced as INVOICE_DATE,
# ap.currency as INVOICE_CURRENCY_CODE,
# 'DISTRIBUTION' as INVOICE_TYPE,
# NULL as TAX_CODE,
# ap.description as DESCRIPTION,
# ap.invoiceHeaderAmount INVOICE_HEADER_AMT,
# ap.totalTaxAmount as TOTAL_TAX_AMOUNT,
# null as PAYMENT_DUE_DATE,
# ap.paymentTermId as TERM,
# 'OPEN' POSTING_STATUS,
# company.name COMPANY_NAME,
# SUM(-1*ap.localAmount) AP_LOC_AMT,
# SUM(-1*(ap.documentAmount / ap.exchangeRateUSD)) as AP_DOC_AMT_WITH_EXCG,
# ap.currency as LOC_CURR_CODE,
# gl_linkage.glJournalId as GL_JOURNAL_ID,
# 'POSTED' STATUS,
# null as INVOICE_CLEARED_DATE,
# ledger.name as LEDGER_NAME,
# date_format(ap.postdate,'yyyy') as POSTED_YEAR,
# referenceDocumentNumber as REF_DOC_NUM,
# null as CLEARING_DOC_NUM,
# 'PAYABLES' as XACT_CODE,
# case when ap.documentTypeId like '%INVDIST%' then 'INVDIST~ITEM' when ap.documentTypeId like '%STANDARD%' THEN 'STANDARD' END AS XACT_TYPE_CODE,
# NULL AS VOID_PAYMENT,
# current_date as W_INSERT_DT
# FROM
# s_finance.ap_transactions_ebs ap
# left join s_core.supplier_account_ebs sup on ap.supplier_id = sup._id
# left join s_core.ledger_ebs ledger on ap.ledger_id = ledger._id
# left join s_core.user_ebs created on created._id = ap.createdby_id
# left join s_core.user_ebs modified on modified._id = ap.modifiedBy_ID
# left join s_core.organization_ebs company on company._id = ap.company_id and company.organizationType ='COMPANY'
#  join (SELECT DISTINCT
#   INT(GLIMPREF.JE_HEADER_ID) || '-' || INT(GLIMPREF.JE_LINE_NUM) glJournalId,
#   REPLACE(STRING(INT(T.LEDGER_ID)), ",", "") ledgerId,
#   AELINE.AE_HEADER_ID || '-' || AELINE.AE_LINE_NUM slaTrxId,
#   DLINK.SOURCE_DISTRIBUTION_TYPE || CASE
#     WHEN DLINK.SOURCE_DISTRIBUTION_TYPE = 'AP_INV_DIST' THEN (
#       CASE
#         WHEN AELINE.ACCOUNTING_CLASS_CODE = 'LIABILITY' THEN '-LIABILITY'
#         ELSE '-EXPENSE'
#       END
#     )
#     ELSE (
#       CASE
#         WHEN DLINK.SOURCE_DISTRIBUTION_TYPE = 'AP_PMT_DIST' THEN '-' || AELINE.ACCOUNTING_CLASS_CODE
#       END
#     )
#   END || '-' || INT(DLINK.SOURCE_DISTRIBUTION_ID_NUM_1)  AS sourceDistributionId
# FROM
#   ebs.GL_LEDGERS T,
#   ebs.GL_PERIODS PER,
#   ebs.GL_JE_HEADERS JHEADER,
#   ebs.GL_IMPORT_REFERENCES GLIMPREF,
#   ebs.XLA_AE_LINES AELINE,
#   ebs.XLA_DISTRIBUTION_LINKS DLINK,
#   ebs.GL_JE_BATCHES JBATCH
# WHERE
#   AELINE.GL_SL_LINK_TABLE = GLIMPREF.GL_SL_LINK_TABLE
#   AND AELINE.GL_SL_LINK_ID = GLIMPREF.GL_SL_LINK_ID
#   AND AELINE.AE_HEADER_ID = DLINK.AE_HEADER_ID
#   AND AELINE.AE_LINE_NUM = DLINK.AE_LINE_NUM
#   AND GLIMPREF.JE_HEADER_ID = JHEADER.JE_HEADER_ID
#   AND JHEADER.JE_BATCH_ID = JBATCH.JE_BATCH_ID
#   AND JHEADER.LEDGER_ID = T.LEDGER_ID
#   AND JHEADER.STATUS = 'P'
#   AND T.PERIOD_SET_NAME = PER.PERIOD_SET_NAME
#   AND JHEADER.PERIOD_NAME = PER.PERIOD_NAME
#   AND DECODE ('N', 'Y', T.LEDGER_ID, 1) IN (1) 
#   AND DECODE ('N', 'Y', T.LEDGER_CATEGORY_CODE, 'NONE') IN ('NONE')
#   AND DLINK.SOURCE_DISTRIBUTION_TYPE IN ('AP_INV_DIST')
#   AND DLINK.APPLICATION_ID = 200
#   AND AELINE.APPLICATION_ID = 200) gl_linkage on gl_linkage.sourcedistributionid = ap.accountdocid --and gl_linkage.ledgerid = ap.ledgerid
#  left join s_core.date on date_format(postdate,'yyyyMMdd') = date.dayid


# where
# ap._deleted = 'false'
# --AND purchaseinvoicenumber = 'K2-0015/2021'
# and date.fiscalYearId >= date_format(add_months(current_date, 6), 'yyyy')-2
# GROUP BY
# ap.purchaseinvoicenumber,
# ap.invoiceId,
# ap.owningBusinessUnitId ,
# date_format(ap.postdate,'MMM-yy') ,
# ap.postDate ,
# sup.supplierNumber,
# sup.supplierName,
# ap.purchaseInvoiceLine,
# modified.login,
# created.login,
# ap.dateInvoiced,
# ap.currency ,
# ap.description,
# ap.invoiceHeaderAmount ,
# ap.totalTaxAmount ,
# ap.paymentTermId,
# company.name ,
# ap.currency,
# gl_linkage.glJournalId ,
# ledger.name,
# date_format(ap.postdate,'yyyy'),
# ap.referenceDocumentNumber,
# case when ap.documentTypeId like '%INVDIST%' then 'INVDIST~ITEM' when ap.documentTypeId like '%STANDARD%' THEN 'STANDARD' END,
# ap.reversalFlag
# --ORDER BY gl_linkage.glJournalId

# COMMAND ----------

# %sql 
# create or replace view g_fin_qv.wc_qv_po_dates as
# SELECT
#   POH.SEGMENT1 AS PO_NUM,
#   PLL.LINE_NUM,
#   INT(POH.IN_TRANSIT_TIME) AS TRANSIT_TIME,
#   PLA.NEED_BY_DATE,
#   DATE_ADD(pla.need_by_date, - INT(POH.IN_TRANSIT_TIME)) RETD,
#   PLA.PROMISED_DATE,
#   (
#     CASE
#       WHEN (
#         date_format (poh.creation_date, 'yyyyMMdd') BETWEEN '20220520'
#         AND date_format(current_date, 'yyyyMMdd')
#         OR date_format (pla.last_update_date, 'yyyyMMdd') BETWEEN '20220520'
#         AND date_format(current_date, 'yyyyMMdd')
#       )
#       and nvl(check_val_emea_po.flag,'N') = 'N'
#       and NVL(check_pto_dropship_so.flag,'N') = 'N' THEN DATE_ADD(PLA.PROMISED_DATE, - INT(POH.IN_TRANSIT_TIME) -21) -- (SELECT apps.fnd_profile.value@OBI_ANSPRD('XX_ANSELL_PO_CETD_UPDATE_DAYS') FROM DUAL)
#       WHEN (
#         date_format (poh.creation_date, 'yyyyMMdd') BETWEEN '20200101'
#         AND '20220519'
#         OR date_format (pla.last_update_date, 'yyyyMMdd') BETWEEN '20200101'
#         AND '20220519'
#       )
#       and NVL(check_val_emea_po.flag,'N') = 'N'
#       and NVL(check_pto_dropship_so.flag,'N') = 'N' THEN DATE_ADD(PLA.PROMISED_DATE, - INT(POH.IN_TRANSIT_TIME) -14)
#       WHEN (NVL(check_val_emea_po.flag,'N') = 'Y')
#       and POH.vendor_id in (1806, 1795, 1798, 162229) THEN DATE_ADD(PLA.PROMISED_DATE, - INT(POH.IN_TRANSIT_TIME))
#       WHEN (NVL(check_val_emea_po.flag,'N') = 'Y')
#       and POH.vendor_id not in (1806, 1795, 1798, 162229) THEN DATE_ADD(
#         rsh.EXPECTED_RECEIPT_DATE,
#         - INT(POH.IN_TRANSIT_TIME)
#       )
#       WHEN (
#         date_format (poh.creation_date, 'yyyyMMdd') BETWEEN '20220520'
#         AND date_format(current_date, 'yyyyMMdd')
#         OR date_format (pla.last_update_date, 'yyyyMMdd') BETWEEN '20220520'
#         AND date_format(current_date, 'yyyyMMdd')
#       )
#       and (NVL(check_pto_dropship_so.flag,'N') = 'Y') THEN DATE_ADD(PLA.PROMISED_DATE, - INT(POH.IN_TRANSIT_TIME) -21) -- (SELECT apps.fnd_profile.value@OBI_ANSPRD('XX_ANSELL_PTO_DS_CETD_UPDATE_DAYS') FROM DUAL)
#       WHEN (
#         date_format (poh.creation_date, 'yyyyMMdd') BETWEEN '20200101'
#         AND '20220519'
#         OR date_format (pla.last_update_date, 'yyyyMMdd') BETWEEN '20200101'
#         AND '20220519'
#       )
#       and (NVL(check_pto_dropship_so.flag,'N') = 'Y') THEN DATE_ADD(PLA.PROMISED_DATE, - INT(POH.IN_TRANSIT_TIME))
#       ELSE DATE_ADD(PLA.PROMISED_DATE, - INT(POH.IN_TRANSIT_TIME))
#     END
#   ) CETD,
#   pla.shipment_num SHIPMENT_NUM
# FROM
#   ebs.PO_HEADERS_ALL POH
#   join ebs.PO_LINES_ALL PLL on POH.PO_HEADER_ID = PLL.PO_HEADER_ID
#   join ebs.po_line_locations_all pla on PLL.PO_LINE_ID = PLA.PO_LINE_ID
#   left join (
#     --check_pto_dropship_so
#     SELECT
#       distinct pha.segment1,
#       'Y' flag
#     FROM
#       ebs.oe_order_headers_all ooh,
#       ebs.oe_transaction_types_tl ott,
#       ebs.oe_drop_ship_sources ods,
#       ebs.po_headers_all pha
#     WHERE
#       1 = 1
#       AND pha.org_id = 938 --AND pha.segment1 = '15191806'
#       AND ott.transaction_type_id = ooh.order_type_id
#       AND ooh.header_id = ods.header_id
#       AND ods.po_header_id = pha.po_header_id
#       AND UPPER (ott.name) IN (
#         SELECT
#           UPPER (meaning)
#         FROM
#           ebs.fnd_lookup_values
#         WHERE
#           enabled_flag = 'Y'
#           AND current_date BETWEEN NVL (
#             start_date_active,
#             current_date
#           )
#           AND NVL (
#             end_date_active,
#             current_date
#           )
#           AND TRIM (UPPER (lookup_type)) = 'XXASL_ROA_DROPSHIP_ORDER_TYPES'
#       )
#   ) check_pto_dropship_so on poh.segment1 = check_pto_dropship_so.segment1
#   left join (
#     --- check_val_emea_po
#     SELECT
#       distinct pha.segment1,
#       'Y' flag
#     FROM
#       ebs.oe_drop_ship_sources ods,
#       ebs.oe_order_headers_all ooh,
#       ebs.oe_order_lines_all ool,
#       ebs.hz_cust_accounts hca,
#       ebs.hz_parties hp,
#       ebs.fnd_lookup_values flv,
#       ebs.po_headers_all pha
#     WHERE
#       pha.org_id = 938
#       AND ods.po_header_id = pha.po_header_id
#       AND ooh.header_id = ods.header_id
#       AND ooh.header_id = ool.header_id
#       AND ooh.sold_to_org_id = hca.cust_account_id
#       AND hp.party_id = hca.party_id
#       AND hca.account_number = flv.lookup_code
#       AND flv.lookup_type = 'XXANS_EMEA_CUSTOMER'
#       AND flv.language = 'US'
#       AND flv.enabled_flag = 'Y' --AND pha.segment1 = p_po_num
#   ) check_val_emea_po on poh.segment1 = check_val_emea_po.segment1
#   left join (
#     select
#       poh.segment1,
#       max(rsh.EXPECTED_RECEIPT_DATE) EXPECTED_RECEIPT_DATE
#     from
#       ebs.rcv_shipment_headers rsh,
#       ebs.rcv_shipment_lines rsl,
#       ebs.po_headers_all poh,
#       ebs.po_lines_all pla
#     where
#       rsh.shipment_header_id = rsl.shipment_header_id
#       and rsl.po_header_id = poh.po_header_id
#       and rsl.PO_LINE_ID = pla.PO_LINE_ID
#       and shipment_line_status_code = 'EXPECTED'
#     group by
#       poh.segment1
#   ) rsh on poh.segment1 = rsh.segment1
# where
#   NVL(POH.CLOSED_CODE, 'OPEN') <> 'CLOSED'
#   AND NVL(PLA.CLOSED_CODE, 'OPEN') <> 'CLOSED'
#   AND NVL(pll.CLOSED_CODE, 'OPEN') <> 'CLOSED'
#   AND NVL (poh.cancel_flag, 'N') <> 'Y'
#   AND NVL (pla.cancel_flag, 'N') <> 'Y'
#   AND NVL (pll.cancel_flag, 'N') <> 'Y'
#   AND poh.type_lookup_code = 'STANDARD'
#   and date_format(poh.creation_date, 'yyyyMMdd') >= '20210701'
#   and PLL.item_id is not null
#   AND poh.org_id IN (select distinct key_value from smartsheets.edm_control_table where table_id = 'WC_QV_PO_DATES')
#   --and POH.SEGMENT1 = '15191806'
  

# COMMAND ----------

# %sql 
# CREATE OR REPLACE VIEW g_fin_qv.WC_QV_GLOBAL_INVENTORY_SHIP_A AS
# SELECT
#   orderline_details.operating_unit OPERATING_UNIT,
#   orderline_details.orderNumber ORDER_NUM,
#   pickline_details.lineNumber ORDER_LINE_NUM,
#   pickline_details.orderItemDetailNumber ORDER_LINE_DETAIL_NUM,
#   invoice_details.invoiceNumber INVOICE_NUMBER,
#   pickline_details.organizationCode CIE,
#   ROUND(pickline_details.qty, 2) QTY_SHIPPED,
#   pickline_details.actualPickDate SHIPPED_DT,
#   orderline_details.accountNumber CUST_NUM,
#   orderline_details.productcode PROD_NUM,
#   orderline_details.PROD_NAME PROD_NAME,
#   orderline_details.productbrand BRAND,
#   orderline_details.productsubbrand SUBBRAND,
#   ROUND(orderline_details.unit_price, 5) UNIT_PRICE,
#   invoice_details.dateinvoiced INVOICE_DT,
#   invoice_details.fiscal_year FISCAL_YEAR,
#   invoice_details.fiscal_period FISCAL_PERIOD,
#   invoice_details.calendar_year CALENDER_YEAR,
#   invoice_details.partysitenumber SHIPTO_SITE,
#   orderline_details.ansstduom STD_UOM,
#   CASE
#     WHEN orderline_details.accountNumber in ('11682', '11683') then 'IC'
#     WHEN invoice_details.partysitenumber IN (
#       '10266',
#       '10382',
#       '10200',
#       '10633',
#       '10881',
#       '11634',
#       '11974',
#       '595133',
#       '368690',
#       '402281',
#       '497486',
#       '368697',
#       '368689'
#     ) THEN 'DS'
#   END DIRECT_SHIPMENT
#  -- ,orderline_details.salesorderDetailId
# from
#   (
#     --orderlines
#     SELECT
#       org.name operating_unit,
#       oh.orderNumber,
#       ol.salesorderDetailId,
#       ol.orderAmount /(ol.quantityordered * ol.ansStdUomConv) AS UNIT_PRICE,
#       ol.quantityordered * ol.ansStdUomConv qty,
#       pr.productcode,
#       pr.name AS PROD_NAME,
#       pr.productBrand,
#       pr.productSubBrand,
#       pr.ansStdUom,
#       ac.accountNumber,
#       ac.name
#     from
#       s_supplychain.sales_order_headers_agg oh
#       join s_supplychain.sales_order_lines_agg ol on oh._ID = ol.salesOrder_ID
#       join s_core.organization_agg org on oh.owningBusinessUnit_ID = org._ID
#       join s_core.organization_agg inv on ol.inventoryWarehouse_ID = inv._ID
#       join s_core.product_agg pr on ol.item_ID = pr._ID
#       join s_core.date date on date_format(ol.actualShipDate, 'yyyyMMdd') = date.dayid
#       join s_core.transaction_type_agg tt on oh.orderType_Id = tt._id
#       join s_core.account_agg ac on oh.customer_ID = ac._ID
#     where
#     oh._SOURCE = 'EBS' and
#       --date_format(ol.actualShipDate,'yyyyMM') = '202103'
#       date.currentPeriodCode in ('Current', 'Previous')
#       and CASE
#         WHEN tt.description LIKE '%Direct Shipment%' THEN 'Direct Shipment'
#         WHEN tt.description LIKE '%Consignment%' THEN 'Consignment'
#         WHEN tt.description LIKE '%Credit Memo%' THEN 'Credit Memo'
#         WHEN tt.description LIKE '%Drop Shipment%' THEN 'Drop Shipment'
#         WHEN tt.description LIKE '%Free Of Charge%' THEN 'Free Of Charge'
#         WHEN tt.description LIKE '%GTO%' THEN 'GTO'
#         WHEN tt.description LIKE '%Intercompany%' THEN 'Intercompany'
#         WHEN tt.description LIKE '%Replacement%' THEN 'Replacement'
#         WHEN tt.description LIKE '%Sample Return%' THEN 'Sample Return'
#         WHEN tt.description LIKE '%Return%' THEN 'Return'
#         WHEN tt.description LIKE '%Sample%' THEN 'Sample'
#         WHEN tt.description LIKE '%Service%' THEN 'Service'
#         WHEN tt.description LIKE '%Standard%' THEN 'Standard'
#         WHEN tt.description LIKE '%Customer_Safety_Stock%' THEN 'Safety Stock'
#         ELSE tt.description
#       end NOT IN (
#         'Safety Stock',
#         'Return',
#         'Sample',
#         'Consignment',
#         'Credit Memo',
#         'Free Of Charge',
#         'Replacement',
#         'Sample Return',
#         'Service'
#       )
#   ) orderline_details
#   join (
#     --picklines
#     SELECT
#       ship.salesorderDetailId,
#       inv.organizationCode,
#       ship.lineNumber,
#       ship.actualPickDate,
#       pr.productcode,
#       sequenceNumber orderItemDetailNumber,
#       sum(ship.quantityShipped * ship.ansStdUomConv) Qty
#     from
#       s_supplychain.sales_shipping_lines_agg ship
#       join s_core.organization_agg inv on ship.inventoryWarehouse_ID = inv._ID
#       join s_core.product_agg pr on ship.item_ID = pr._ID
#       join s_core.date date on date_format(ship.actualPickDate, 'yyyyMMdd') = date.dayid
#     where
#     ship._SOURCE = 'EBS'
#       --date_format(ship.actualShipDate,'yyyyMM') =  '202103'
#      AND date.currentPeriodCode in ('Current', 'Previous')
#       AND inv.organizationCode IN (
#         '508',
#         '509',
#         '511',
#         '532',
#         '800',
#         '805',
#         '325',
#         '811',
#         '832',
#         '600',
#         '601',
#         '605',
#         '401',
#         '802',
#         '819',
#         '602',
#         '724',
#         '826',
#         '827'
#       )
#     group by
#       ship.salesorderDetailId,
#       inv.organizationCode,
#       ship.lineNumber,
#       ship.actualPickDate,
#       pr.productcode,
#       ship.sequenceNumber
#   ) pickline_details on pickline_details.salesorderDetailId = orderline_details.salesorderDetailId
#   left join (
#     --invoicelines
#     select
#       int(float(invl.salesorderDetailId)) salesorderDetailId,
#       invh.invoiceNumber,
#       invh.dateInvoiced,
#       date.fiscalYearId fiscal_year,
#       date.periodName fiscal_period,
#       date.yearId calendar_year,
#       shipto.partysitenumber
#     from
#       s_supplychain.sales_invoice_lines_agg invl
#       join s_supplychain.sales_invoice_headers_agg invh on invl.invoice_ID = invh._ID
#       join s_core.customer_location_agg shipTo on invl.shipToAddress_ID = shipto._ID
#       join s_core.date date on date_format(invl.actualShipDate, 'yyyyMMdd') = date.dayid
#     where
#       invh._SOURCE = 'EBS'
#       and invl._SOURCE = 'EBS' -- and date_format(invl.actualShipDate,'yyyyMM') =   '202103'
#       AND date.currentPeriodCode in ('Current', 'Previous')
#       and invl.salesorderDetailId is not null
#   ) invoice_details on invoice_details.salesorderDetailId = orderline_details.salesorderDetailId
#   --where orderline_details.orderNumber='1488287'

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW g_fin_qv.wc_qv_invoices_shipments_v AS
# MAGIC select
# MAGIC   returnFlag,
# MAGIC   discountlineFlag,
# MAGIC   transactionTypeCode,
# MAGIC   CMP_NUM_DETAIL,
# MAGIC   CUSTOMER_ID,
# MAGIC   SHIPTO_DLY_LOC_ID,
# MAGIC   ITEM_BRANCH_KEY,
# MAGIC   ORDER_NUMBER,
# MAGIC   ORDER_LINE_NUMBER,
# MAGIC   INVOICE_NUMBER,
# MAGIC   SHIP_DATE,
# MAGIC   INVOICE_DATE,
# MAGIC   ITEM_NUMBER,
# MAGIC   ANSELL_STD_UM,
# MAGIC   SALES_QUANTITY,
# MAGIC   CURRENCY,
# MAGIC   GROSS_SALES_AMOUNT_LC,
# MAGIC   RETURNS_LC,
# MAGIC   SETTLEMENT_DISCOUNT_LC,
# MAGIC   SALES_COST_AMOUNT_LC,
# MAGIC   DOC_TYPE,
# MAGIC   TRANS_TYPE,
# MAGIC   SALES_COST_AMOUNT_LE,
# MAGIC   DOC_CURR_CODE,
# MAGIC   GROSS_SALES_AMOUNT_DOC,        
# MAGIC   RETURNS_DOC,
# MAGIC   SETTLEMENT_DISCOUNT_DOC,
# MAGIC   NET_SALES_DOC,
# MAGIC   SALES_COST_AMOUNT_DOC,
# MAGIC   SALES_COST_AMOUNT_LE_DOC,
# MAGIC   CUST_PO_NUM,
# MAGIC   SOURCE_NAME
# MAGIC from
# MAGIC   g_fin_qv.wc_qv_invoices_shipments
