# Databricks notebook source
# MAGIC %sql
# MAGIC create database if not exists g_emea_cs

# COMMAND ----------

# MAGIC %sql 
# MAGIC CREATE OR REPLACE VIEW g_emea_cs.emea_sales_orders_v AS
# MAGIC SELECT
# MAGIC   order_lines.createdBy createdByLine,
# MAGIC   order_headers.createdBy createdByHeader,
# MAGIC   order_lines._SOURCE,
# MAGIC   order_lines.item_id,
# MAGIC   order_headers.owningBusinessUnit_Id,
# MAGIC   order_lines.inventoryWarehouse_Id,
# MAGIC   order_headers.customer_Id,
# MAGIC   order_headers.orderDate,
# MAGIC   order_headers.orderNumber,
# MAGIC   nvl(cast(order_headers.orderNumber as integer), order_headers.orderNumber) orderNumberStripped,
# MAGIC   order_headers.baseCurrencyId,
# MAGIC   order_headers.transactionCurrencyId,
# MAGIC   order_lines.orderUomCode,
# MAGIC   order_lines.productCode,
# MAGIC   order_headers.exchangeRate,
# MAGIC   order_headers.exchangeRateUsd,
# MAGIC   round(sum(order_lines.quantityOrdered), 0) quantityOrdered,
# MAGIC   round(sum(order_lines.quantityBackOrdered), 0) quantityBackOrdered,
# MAGIC   round(sum(order_lines.orderAmount), 2) orderValueTransactionCurrency,
# MAGIC   order_headers.distributionChannel,
# MAGIC   organization.organizationCode,
# MAGIC   order_headers.customerServiceRepresentative as csr,
# MAGIC   order_headers.orderEntryType orderSource,
# MAGIC   order_headers.orderHoldType as blockingReason,
# MAGIC   nvl(order_headers.orderHoldDate1, '1900-01-01') as blockedDate,
# MAGIC   order_lines.orderLineStatus,
# MAGIC   order_lines.reasonForRejection
# MAGIC from
# MAGIC   s_supplychain.sales_order_headers_agg order_headers
# MAGIC   inner join s_supplychain.sales_order_lines_agg order_lines on order_headers._ID = order_lines.salesOrder_ID
# MAGIC   inner join s_core.organization_agg organization on order_headers.owningBusinessUnit_ID = organization._ID
# MAGIC   left outer join s_core.account_agg accounts on order_headers.customer_id = accounts._ID
# MAGIC   inner join s_core.transaction_type_agg tt on order_headers.orderType_ID = tt._ID
# MAGIC where
# MAGIC   1 = 1
# MAGIC   and (
# MAGIC     order_headers._SOURCE in  ('KING', 'SAP', 'PRMS')
# MAGIC     or organization.organizationId in ('3818')
# MAGIC   )
# MAGIC   and not order_lines._DELETED
# MAGIC   and not order_headers._DELETED
# MAGIC   and not tt.sampleOrderFlag
# MAGIC group by
# MAGIC   order_lines.createdBy,
# MAGIC   order_lines._SOURCE,
# MAGIC   order_lines.item_id,
# MAGIC   order_headers.owningBusinessUnit_ID,
# MAGIC   order_headers.orderDate,
# MAGIC   order_headers.orderNumber,
# MAGIC   order_headers.baseCurrencyId,
# MAGIC   order_headers.transactionCurrencyId,
# MAGIC   order_lines.orderUomCode,
# MAGIC   order_lines.productCode,
# MAGIC   order_lines.inventoryWarehouse_ID,
# MAGIC   order_headers.customer_Id,
# MAGIC   order_headers.exchangeRate,
# MAGIC   order_headers.exchangeRateUsd,
# MAGIC   order_headers.distributionChannel,
# MAGIC   organization.organizationCode,
# MAGIC   order_headers.customerServiceRepresentative,
# MAGIC   order_headers.orderentrytype,
# MAGIC   order_headers.createdBy,
# MAGIC   order_headers.orderHoldType,
# MAGIC   order_headers.orderHoldDate1,
# MAGIC   order_lines.orderLineStatus,
# MAGIC   order_lines.reasonForRejection

# COMMAND ----------

# MAGIC %sql 
# MAGIC CREATE OR REPLACE VIEW g_emea_cs.emea_sales_invoices_v AS
# MAGIC SELECT
# MAGIC   invoice_headers._SOURCE,
# MAGIC   invoice_lines.item_Id,
# MAGIC   invoice_headers.owningBusinessUnit_Id,
# MAGIC   invoice_lines.inventoryWarehouse_Id,
# MAGIC   invoice_headers.customer_Id,
# MAGIC   invoice_headers.dateInvoiced,
# MAGIC   invoice_headers.invoiceNumber,
# MAGIC   invoice_headers.baseCurrencyId,
# MAGIC   invoice_headers.transactionCurrencyId,
# MAGIC   invoice_lines.orderUomCode,
# MAGIC   invoice_lines.productCode,
# MAGIC   invoice_headers.exchangeRate,
# MAGIC   invoice_headers.exchangeRateUsd,
# MAGIC   sum(invoice_lines.quantityInvoiced) quantityInvoiced,
# MAGIC   sum(invoice_lines.baseAmount) baseAmountDocCurr
# MAGIC from
# MAGIC   s_supplychain.sales_invoice_headers_agg invoice_headers
# MAGIC   inner join s_supplychain.sales_invoice_lines_agg invoice_lines on invoice_headers._ID = invoice_lines.invoice_ID
# MAGIC   inner join s_core.organization_agg organization on invoice_headers.owningBusinessUnit_ID = organization._ID
# MAGIC   left outer join s_core.account_agg accounts on invoice_headers.customer_id = accounts._ID
# MAGIC where
# MAGIC   1=1
# MAGIC   and year(invoice_headers.dateInvoiced) >= year(current_date) - 2
# MAGIC   and (invoice_headers._SOURCE in  ('KING', 'SAP', 'PRMS')
# MAGIC     or organization.organizationId in ('3818'))
# MAGIC   and not invoice_headers._DELETED
# MAGIC   and not invoice_lines._DELETED
# MAGIC group by
# MAGIC   invoice_headers._SOURCE,
# MAGIC   invoice_lines.item_id,
# MAGIC   invoice_headers.owningBusinessUnit_ID,
# MAGIC   invoice_lines.inventoryWarehouse_ID,
# MAGIC   invoice_headers.customer_Id,
# MAGIC   invoice_headers.dateInvoiced,
# MAGIC   invoice_headers.invoiceNumber,
# MAGIC   invoice_headers.baseCurrencyId,
# MAGIC   invoice_headers.transactionCurrencyId,
# MAGIC   invoice_lines.orderUomCode,
# MAGIC   invoice_lines.productCode,
# MAGIC   invoice_headers.exchangeRate,
# MAGIC   invoice_headers.exchangeRateUsd
# MAGIC  

# COMMAND ----------

# MAGIC %sql 
# MAGIC CREATE OR REPLACE VIEW g_emea_cs.emea_product_v AS
# MAGIC select
# MAGIC   _ID,
# MAGIC   _SOURCE,
# MAGIC   gbu,
# MAGIC   itemId,
# MAGIC   name,
# MAGIC   productCode,
# MAGIC   productDivision
# MAGIC from
# MAGIC   s_core.product_agg
# MAGIC where
# MAGIC   _source in ('KING', 'SAP', 'PRMS', 'EBS')

# COMMAND ----------

# MAGIC %sql 
# MAGIC CREATE OR REPLACE VIEW g_emea_cs.emea_organization_v AS
# MAGIC select 
# MAGIC _ID,
# MAGIC _SOURCE,
# MAGIC organizationId,
# MAGIC organizationCode,
# MAGIC name,
# MAGIC organizationType
# MAGIC from s_core.organization_agg c
# MAGIC where
# MAGIC   _source in ('KING', 'SAP', 'PRMS', 'EBS')

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW g_emea_cs.emea_cs_account_v AS
# MAGIC select 
# MAGIC _ID,
# MAGIC _SOURCE,
# MAGIC accountId,
# MAGIC accountNumber,
# MAGIC name
# MAGIC from s_core.account_agg
# MAGIC where 
# MAGIC _source in ('KING', 'SAP', 'PRMS', 'EBS')
