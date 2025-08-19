# Databricks notebook source
# MAGIC %sql
# MAGIC create database if not exists g_na_cs

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW  g_na_cs.SALES_FACT_SUMMARY AS
# MAGIC SELECT 

# COMMAND ----------

# MAGIC %sql
# MAGIC select 
# MAGIC   invh.invoiceNumber Invoice_num,
# MAGIC   COUNT(invoiceNumber) AS NumberInvoicesMemos
# MAGIC from 
# MAGIC   s_supplychain.sales_invoice_headers_agg invh
# MAGIC   join s_supplychain.sales_invoice_lines_agg invl
# MAGIC     on invh._ID = invl.invoice_ID
# MAGIC   join s_core.organization_agg ou
# MAGIC     on invh.owningBusinessUnit_ID = ou._id
# MAGIC   join s_core.transactiontype_agg trtype on invh.transaction_ID = trtype._ID
# MAGIC where 
# MAGIC   invh._source = 'EBS'
# MAGIC   and ou.name NOT IN ('SXWELL USA LLC','ANSELL GLOBAL TRADING CENTER (MALAYSIA) SDN. BHD.(1088855-W)','Ansell India Protective Products Private Limited')
# MAGIC   and trtype.transactionTypeCode in ('INV', 'CM')
# MAGIC group by 
# MAGIC   invoiceNumber

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from s_core.

# COMMAND ----------

# MAGIC %sql
# MAGIC select   * from  s_supplychain.sales_order_headers_agg invh

# COMMAND ----------

# MAGIC %sql 
# MAGIC CREATE OR REPLACE VIEW g_emea_cs.emea_sales_orders_v AS
# MAGIC SELECT
# MAGIC   order_lines._SOURCE,
# MAGIC   order_lines.item_id,
# MAGIC   order_headers.owningBusinessUnit_Id,
# MAGIC   order_lines.inventoryWarehouse_Id,
# MAGIC   order_headers.customer_Id,
# MAGIC   order_headers.orderDate,
# MAGIC  -- organization.name organizationName,
# MAGIC   order_headers.orderNumber,
# MAGIC   order_headers.baseCurrencyId,
# MAGIC   order_headers.transactionCurrencyId,
# MAGIC   order_lines.orderUomCode,
# MAGIC   order_lines.productCode,
# MAGIC   --accounts.name,
# MAGIC   order_headers.exchangeRate,
# MAGIC   order_headers.exchangeRateUsd,
# MAGIC   round(sum(order_lines.quantityOrdered), 0) quantityOrdered,
# MAGIC   round(sum(order_lines.quantityBackOrdered), 0) quantityBackOrdered,
# MAGIC   round(sum(order_lines.orderAmount), 2) orderValueTransactionCurrency,
# MAGIC   order_headers.distributionChannel,
# MAGIC   organization.organizationCode
# MAGIC from
# MAGIC   s_supplychain.sales_order_headers_agg order_headers
# MAGIC   inner join s_supplychain.sales_order_lines_agg order_lines on order_headers._ID = order_lines.salesOrder_ID
# MAGIC   inner join s_core.organization_agg organization on order_headers.owningBusinessUnit_ID = organization._ID 
# MAGIC   left outer join s_core.account_agg accounts on order_headers.customer_id = accounts._ID
# MAGIC where
# MAGIC   1 = 1
# MAGIC  -- and year(order_headers.orderDate) >= year(current_date) - 2
# MAGIC   and (
# MAGIC     order_headers._SOURCE in  ('KING', 'SAP', 'PRMS')
# MAGIC     or organization.organizationId in ('3818')
# MAGIC   ) 
# MAGIC   and  order_headers._SOURCE in  ('SAP')
# MAGIC   --and organization.organizationCode not in ('ZCMS')
# MAGIC group by
# MAGIC   order_lines._SOURCE,
# MAGIC   order_lines.item_id,
# MAGIC   order_headers.owningBusinessUnit_ID,
# MAGIC   order_headers.orderDate,
# MAGIC  -- organization.name,
# MAGIC   order_headers.orderNumber,
# MAGIC   order_headers.baseCurrencyId,
# MAGIC   order_headers.transactionCurrencyId,
# MAGIC   order_lines.orderUomCode,
# MAGIC   order_lines.productCode,
# MAGIC   order_lines.inventoryWarehouse_ID,
# MAGIC   order_headers.customer_Id,
# MAGIC   order_headers.exchangeRate,
# MAGIC   order_headers.exchangeRateUsd,
# MAGIC   --accounts.name
# MAGIC   order_headers.distributionChannel,
# MAGIC   organization.organizationCode

# COMMAND ----------

# MAGIC %sql 
# MAGIC CREATE OR REPLACE VIEW g_emea_cs.emea_sales_invoices_v AS
# MAGIC SELECT
# MAGIC   invoice_headers._SOURCE,
# MAGIC   invoice_lines.item_iD,
# MAGIC   invoice_headers.owningBusinessUnit_Id,
# MAGIC   invoice_lines.inventoryWarehouse_Id,
# MAGIC   invoice_headers.customer_Id,
# MAGIC   invoice_headers.dateInvoiced,
# MAGIC   --organization.name organizationName,
# MAGIC   invoice_headers.invoiceNumber,
# MAGIC   invoice_headers.baseCurrencyId,
# MAGIC   invoice_headers.transactionCurrencyId,
# MAGIC   invoice_lines.orderUomCode,
# MAGIC   invoice_lines.productCode,
# MAGIC   invoice_headers.exchangeRate,
# MAGIC   invoice_headers.exchangeRateUsd,
# MAGIC   --accounts.name,
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
# MAGIC group by
# MAGIC   invoice_headers._SOURCE,
# MAGIC   invoice_lines.item_id,
# MAGIC   invoice_headers.owningBusinessUnit_ID,
# MAGIC   invoice_lines.inventoryWarehouse_ID,
# MAGIC   invoice_headers.customer_Id,
# MAGIC   invoice_headers.dateInvoiced,
# MAGIC  -- organization.name,
# MAGIC   invoice_headers.invoiceNumber,
# MAGIC   invoice_headers.baseCurrencyId,
# MAGIC   invoice_headers.transactionCurrencyId,
# MAGIC   invoice_lines.orderUomCode,
# MAGIC   invoice_lines.productCode,
# MAGIC   invoice_headers.exchangeRate,
# MAGIC   invoice_headers.exchangeRateUsd--,
# MAGIC  -- accounts.name

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
