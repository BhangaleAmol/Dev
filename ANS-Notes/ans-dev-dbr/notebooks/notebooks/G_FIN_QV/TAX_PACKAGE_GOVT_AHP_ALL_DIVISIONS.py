# Databricks notebook source
# MAGIC %sql 
# MAGIC CREATE OR REPLACE VIEW g_fin_qv.tax_package_govt_ahp_all_divisions AS
# MAGIC select
# MAGIC   date_format(sales_invoice_headers.dateInvoiced, 'yyyy') INVOICED_YEAR,
# MAGIC   date_format(sales_invoice_headers.dateInvoiced, 'yyyyMM') INVOICED_MONTH,
# MAGIC   --sales_invoice_headers.invoiceNumber,
# MAGIC   inv.organizationCode INVENTORY_ORG_CODE,
# MAGIC   inv.name INVENTORY_ORG_NAME,
# MAGIC   org.name EBS_OPERATING_UNIT,
# MAGIC   CASE
# MAGIC     WHEN inv.organizationCode IN ('400', '401', '403') THEN 'Ansell Canada Inc'
# MAGIC     WHEN inv.organizationCode IN ('514', '518', '809') THEN 'Ansell Mexico'
# MAGIC     WHEN inv.organizationCode IN (
# MAGIC       '817',
# MAGIC       '821',
# MAGIC       '803',
# MAGIC       '819',
# MAGIC       '826',
# MAGIC       '834',
# MAGIC       '800',
# MAGIC       '801',
# MAGIC       '828',
# MAGIC       '811',
# MAGIC       '804',
# MAGIC       '802',
# MAGIC       '355',
# MAGIC       '823',
# MAGIC       '805',
# MAGIC       '832',
# MAGIC       '810',
# MAGIC       '807',
# MAGIC       '804',
# MAGIC       '822',
# MAGIC       '814',
# MAGIC       '813',
# MAGIC       '824',
# MAGIC       '553',
# MAGIC       '450',
# MAGIC       '554',
# MAGIC       '000'
# MAGIC     ) THEN 'Ansell Healthcare Products LLC'
# MAGIC WHEN inv.organizationCode IN ('517',
# MAGIC '511',
# MAGIC '503',
# MAGIC '501',
# MAGIC '504',
# MAGIC '502',
# MAGIC '532',
# MAGIC '513') THEN 'Ansell Protective Products Inc'
# MAGIC
# MAGIC     WHEN inv.organizationCode IN ('601', '600', '602', '605') THEN 'SXWELL USA LLC'
# MAGIC     WHEN inv.organizationCode IN ('325', '326', '327') THEN 'ANSELL GLOBAL TRADING CENTER (MALAYSIA) SDN. BHD.(1088855-W)'
# MAGIC     WHEN inv.organizationCode IN ('505', '506', '507', '508', '509') THEN 'Ansell Hawkeye Inc'
# MAGIC     WHEN inv.organizationCode IN ('724') THEN 'Ansell India Protective Products Private Limited'
# MAGIC   END INV_ORG_OU_NAME,
# MAGIC
# MAGIC CASE
# MAGIC     WHEN inv.organizationCode IN ('400', '401', '403') THEN 'Ansell Canada'
# MAGIC     WHEN inv.organizationCode IN ('514', '518', '809') THEN 'Ansell Latin America'
# MAGIC     WHEN inv.organizationCode IN (
# MAGIC       '817',
# MAGIC       '821',
# MAGIC       '803',
# MAGIC       '819',
# MAGIC       '826',
# MAGIC       '834',
# MAGIC       '800',
# MAGIC       '801',
# MAGIC       '828',
# MAGIC       '811',
# MAGIC       '804',
# MAGIC       '802',
# MAGIC       '355',
# MAGIC       '823',
# MAGIC       '805',
# MAGIC       '832',
# MAGIC       '810',
# MAGIC       '807',
# MAGIC       '804',
# MAGIC       '822',
# MAGIC       '814',
# MAGIC       '813',
# MAGIC       '824',
# MAGIC       '553',
# MAGIC       '450',
# MAGIC       '554',
# MAGIC       '000'
# MAGIC     ) THEN 'Ansell US'
# MAGIC WHEN inv.organizationCode IN ('517',
# MAGIC '511',
# MAGIC '503',
# MAGIC '501',
# MAGIC '504',
# MAGIC '502',
# MAGIC '532',
# MAGIC '513') THEN 'Ansell US'
# MAGIC     WHEN inv.organizationCode IN ('601', '600', '602', '605') THEN 'Ansell US'
# MAGIC     WHEN inv.organizationCode IN ('325', '326', '327') THEN 'Ansell Asia Pacific'
# MAGIC     WHEN inv.organizationCode IN ('505', '506', '507', '508', '509') THEN 'Ansell US'
# MAGIC     WHEN inv.organizationCode IN ('724') THEN 'Ansell Asia Pacific'
# MAGIC   END LEGAL_ENTITY_NAME,
# MAGIC   account.customerType EXTERNAL_INTERNAL,
# MAGIC   org.organizationCode AS CMP_NUM_DETAIL,
# MAGIC   account.name NAME,
# MAGIC   product.productdivision PRODUCT_DIVISION,
# MAGIC   account.industry CUSTOMER_INDUSTRY,
# MAGIC   account.subIndustry CUSTOMER_SUBINDUSTRY,
# MAGIC   sales_invoice_lines.warehouseCode AS ITEM_BRANCH_KEY,
# MAGIC    tt_invl.name ORDER_TYPE,
# MAGIC   shipTo.country SHIP_TO_COUNTRY,
# MAGIC   shipTo.county COUNTY,
# MAGIC   shipTo.state SHIP_TO_STATE,
# MAGIC   SUM(
# MAGIC     CASE
# MAGIC       WHEN sales_invoice_lines.returnFlag = 'Y'
# MAGIC       and tt_invh.transactionTypeCode in ('CM') then 0
# MAGIC       WHEN sales_invoice_lines.returnFlag = 'Y'
# MAGIC       and tt_invh.transactionTypeCode not in ('CM') then sales_invoice_lines.baseAmount * sales_invoice_headers.exchangeRateUSD
# MAGIC       WHEN sales_invoice_lines.returnFlag <> 'Y' then sales_invoice_lines.baseAmount * sales_invoice_headers.exchangeRateUSD
# MAGIC     end
# MAGIC   ) as GROSS_SALES_AMOUNT_USD,
# MAGIC   NVL(
# MAGIC     SUM(
# MAGIC       (
# MAGIC         sales_invoice_lines.baseAmount - sales_invoice_lines.settlementDiscountEarned
# MAGIC       ) * sales_invoice_headers.exchangeRateUSD
# MAGIC     ),
# MAGIC     0
# MAGIC   ) NET_SALES_AMOUNT_USD,
# MAGIC   SUM(
# MAGIC     (
# MAGIC       CASE
# MAGIC         WHEN sales_invoice_lines.discountLineFlag = 'N' then sales_invoice_lines.seeThruCost
# MAGIC       end
# MAGIC     ) * sales_invoice_lines.exchangeRate
# MAGIC   ) as SEE_THROUGH_COST_DOC_CURR,
# MAGIC   SUM(
# MAGIC     CASE
# MAGIC       WHEN sales_invoice_lines.returnFlag = 'Y' THEN - sales_invoice_lines.baseAmount
# MAGIC       ELSE 0
# MAGIC     END
# MAGIC   ) as RETURNS_DOC_CURR
# MAGIC from
# MAGIC   s_supplychain.sales_invoice_lines_agg sales_invoice_lines
# MAGIC   join s_supplychain.sales_invoice_headers_agg sales_invoice_headers on sales_invoice_lines.invoice_ID = sales_invoice_headers._ID
# MAGIC   join s_core.organization_agg org on sales_invoice_headers.owningbusinessunit_id = org._ID
# MAGIC   join s_core.organization_agg inv on sales_invoice_lines.inventoryWarehouse_ID = inv._ID
# MAGIC   join s_core.account_agg account on sales_invoice_headers.customer_ID = account._ID
# MAGIC   join s_core.product_agg product on sales_invoice_lines.item_ID = product._ID
# MAGIC   join s_core.customer_location_agg shipTo on sales_invoice_headers.shipToAddress_ID = shipto._ID
# MAGIC   join s_core.transaction_type_agg tt_invh on sales_invoice_headers.transaction_ID = tt_invh._id
# MAGIC   join s_core.transaction_type_agg tt_invl on sales_invoice_lines.orderType_ID = tt_invl._id
# MAGIC   join s_core.date date on date_format(sales_invoice_headers.dateInvoiced, 'yyyyMMdd') = date.dayid
# MAGIC where
# MAGIC   sales_invoice_headers._SOURCE = 'EBS'
# MAGIC   and sales_invoice_lines._SOURCE = 'EBS'
# MAGIC   --and account.customerType = 'External' --and sales_invoice_headers.invoiceNumber in ('21448485')
# MAGIC   -- and date.currentPeriodCode in ('Current','Previous')
# MAGIC   -- and product.productCode <> 'unknown'
# MAGIC   --and account.industry = 'OEM, TENDER & PUBLIC SECTOR'
# MAGIC   --and account.subindustry not in ('OTHER FOR PROFIT')
# MAGIC   and tt_invl.name not in ('unknown')
# MAGIC   --AND date_format(sales_invoice_headers.dateInvoiced, 'yyyyMM') = '202204'
# MAGIC GROUP BY
# MAGIC   --sales_invoice_headers.invoiceNumber,
# MAGIC   sales_invoice_lines.warehouseCode,
# MAGIC   org.organizationCode,
# MAGIC   inv.organizationCode,
# MAGIC   inv.name,
# MAGIC   org.name,
# MAGIC   account.customerType,
# MAGIC   product.productdivision,
# MAGIC   account.industry,
# MAGIC   account.subindustry,
# MAGIC   CASE
# MAGIC     WHEN inv.organizationCode IN ('400', '401', '403') THEN 'Ansell Canada Inc'
# MAGIC     WHEN inv.organizationCode IN ('514', '518', '809') THEN 'Ansell Mexico'
# MAGIC     WHEN inv.organizationCode IN (
# MAGIC       '817',
# MAGIC       '821',
# MAGIC       '803',
# MAGIC       '819',
# MAGIC       '826',
# MAGIC       '834',
# MAGIC       '800',
# MAGIC       '801',
# MAGIC       '828',
# MAGIC       '811',
# MAGIC       '804',
# MAGIC       '802',
# MAGIC       '355',
# MAGIC       '823',
# MAGIC       '805',
# MAGIC       '832',
# MAGIC       '810',
# MAGIC       '807',
# MAGIC       '804',
# MAGIC       '822',
# MAGIC       '814',
# MAGIC       '813',
# MAGIC       '824',
# MAGIC       '553',
# MAGIC       '450',
# MAGIC       '554',
# MAGIC       '000'
# MAGIC     ) THEN 'Ansell Healthcare Products LLC'
# MAGIC WHEN inv.organizationCode IN ('517',
# MAGIC '511',
# MAGIC '503',
# MAGIC '501',
# MAGIC '504',
# MAGIC '502',
# MAGIC '532',
# MAGIC '513') THEN 'Ansell Protective Products Inc'
# MAGIC
# MAGIC     WHEN inv.organizationCode IN ('601', '600', '602', '605') THEN 'SXWELL USA LLC'
# MAGIC     WHEN inv.organizationCode IN ('325', '326', '327') THEN 'ANSELL GLOBAL TRADING CENTER (MALAYSIA) SDN. BHD.(1088855-W)'
# MAGIC     WHEN inv.organizationCode IN ('505', '506', '507', '508', '509') THEN 'Ansell Hawkeye Inc'
# MAGIC     WHEN inv.organizationCode IN ('724') THEN 'Ansell India Protective Products Private Limited'
# MAGIC   END ,
# MAGIC
# MAGIC CASE
# MAGIC     WHEN inv.organizationCode IN ('400', '401', '403') THEN 'Ansell Canada'
# MAGIC     WHEN inv.organizationCode IN ('514', '518', '809') THEN 'Ansell Latin America'
# MAGIC     WHEN inv.organizationCode IN (
# MAGIC       '817',
# MAGIC       '821',
# MAGIC       '803',
# MAGIC       '819',
# MAGIC       '826',
# MAGIC       '834',
# MAGIC       '800',
# MAGIC       '801',
# MAGIC       '828',
# MAGIC       '811',
# MAGIC       '804',
# MAGIC       '802',
# MAGIC       '355',
# MAGIC       '823',
# MAGIC       '805',
# MAGIC       '832',
# MAGIC       '810',
# MAGIC       '807',
# MAGIC       '804',
# MAGIC       '822',
# MAGIC       '814',
# MAGIC       '813',
# MAGIC       '824',
# MAGIC       '553',
# MAGIC       '450',
# MAGIC       '554',
# MAGIC       '000'
# MAGIC     ) THEN 'Ansell US'
# MAGIC WHEN inv.organizationCode IN ('517',
# MAGIC '511',
# MAGIC '503',
# MAGIC '501',
# MAGIC '504',
# MAGIC '502',
# MAGIC '532',
# MAGIC '513') THEN 'Ansell US'
# MAGIC     WHEN inv.organizationCode IN ('601', '600', '602', '605') THEN 'Ansell US'
# MAGIC     WHEN inv.organizationCode IN ('325', '326', '327') THEN 'Ansell Asia Pacific'
# MAGIC     WHEN inv.organizationCode IN ('505', '506', '507', '508', '509') THEN 'Ansell US'
# MAGIC     WHEN inv.organizationCode IN ('724') THEN 'Ansell Asia Pacific'
# MAGIC   END ,
# MAGIC   date_format(sales_invoice_headers.dateInvoiced, 'yyyy'),
# MAGIC   date_format(sales_invoice_headers.dateInvoiced, 'yyyyMM'),
# MAGIC    tt_invl.name ,
# MAGIC   shipTo.country ,
# MAGIC   shipTo.state ,
# MAGIC   account.name,
# MAGIC   shipTo.county
