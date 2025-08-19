# Databricks notebook source
# MAGIC %sql
# MAGIC create database if not exists g_global_fin

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace view g_global_fin.fin_sales_orders_v as
# MAGIC select
# MAGIC   oh._source,
# MAGIC   --ol.salesorderdetailid,
# MAGIC   --ol._ID salesOrderDetail_ID,
# MAGIC   oh.orderDate,
# MAGIC   year(oh.orderDate) orderedYear,
# MAGIC   year(oh.orderDate) || ' / ' || month(oh.orderDate) OrderedMonth,
# MAGIC   int(date_format(oh.orderDate, 'yyyyMMdd')) orderDateId,
# MAGIC   ol.requestDeliveryBy,
# MAGIC  -- ol.scheduledShipDate,
# MAGIC   year(ol.requestDeliveryBy) requestYear,
# MAGIC   year(ol.requestDeliveryBy) || ' / ' || month(ol.requestDeliveryBy) RequestMonth,
# MAGIC   oh.ordernumber,
# MAGIC  
# MAGIC   sum(round(ol.quantityOrdered * ol.caseUomConv, 2)) quantityOrderedCase,
# MAGIC   sum(round(ol.quantityOrdered * ol.ansStdUomConv, 2)) quantityOrderedAnsStduom,
# MAGIC   sum(round(ol.orderAmount, 2)) orderAmount,
# MAGIC   sum(round(ol.orderAmount / oh.exchangeRateUsd, 2)) orderAmountUsd,
# MAGIC   --sum(round(ol.orderAmount / oh.exchangeSpotRate, 2)) orderAmountUsd,
# MAGIC   --ol.apacScheduledShipDate,
# MAGIC   --ol.orderUomCode,
# MAGIC   oh.orderHeaderStatus,
# MAGIC   oh.customer_ID,
# MAGIC   oh.orderType_ID,
# MAGIC   oh.owningBusinessUnit_ID operatingUnit_ID,
# MAGIC   ol.inventoryWarehouse_ID inventoryUnit_ID,
# MAGIC   ol.item_id product_ID,
# MAGIC   oh.party_ID,
# MAGIC   oh.customer_ID account_ID,
# MAGIC   ol.shipToAddress_ID,
# MAGIC   oh.billToAddress_ID,
# MAGIC   sum(ol.quantityInvoiced) quantityInvoiced,
# MAGIC   sum(round(ol.quantityInvoiced * pricePerUnit , 2)) invoicedAmount,
# MAGIC   sum(round(pricePerUnit *  quantityInvoiced / oh.exchangeRateUsd, 2)) invoicedAmountUsd
# MAGIC from
# MAGIC   s_supplychain.sales_order_headers_agg oh
# MAGIC   join s_supplychain.sales_order_lines_agg ol on ol.salesorder_id = oh._ID
# MAGIC   join s_core.TRANSACTION_TYPE_AGG ot on oh.orderType_ID = ot._ID
# MAGIC   join s_core.organization_agg inv on ol.inventoryWarehouse_ID = inv._ID
# MAGIC   join s_core.user_agg createdByHdr on oh.createdBy_ID = createdByHdr._ID
# MAGIC where
# MAGIC   oh._source in ('EBS', 'SAP', 'KGD', 'TOT', 'COL')
# MAGIC   and not OL._DELETED
# MAGIC   and not OH._DELETED
# MAGIC   and not ot._DELETED
# MAGIC   and year(oh.orderDate)* 100 + month(oh.orderDate) >= (year(current_date) - 4) * 100 + 7
# MAGIC group by
# MAGIC   oh.orderDate,
# MAGIC   year(oh.orderDate) ,
# MAGIC   year(oh.orderDate) || ' / ' || month(oh.orderDate) ,
# MAGIC   int(date_format(oh.orderDate, 'yyyyMMdd')) ,
# MAGIC   ol.requestDeliveryBy,
# MAGIC   year(ol.requestDeliveryBy) ,
# MAGIC   year(ol.requestDeliveryBy) || ' / ' || month(ol.requestDeliveryBy) ,
# MAGIC   oh.ordernumber,
# MAGIC   oh.orderHeaderStatus,
# MAGIC   oh.customer_ID,
# MAGIC   oh.orderType_ID,
# MAGIC   oh.owningBusinessUnit_ID ,
# MAGIC   ol.inventoryWarehouse_ID ,
# MAGIC   ol.item_id ,
# MAGIC   oh.party_ID,
# MAGIC   oh.customer_ID ,
# MAGIC   ol.shipToAddress_ID,
# MAGIC   oh.billToAddress_ID,
# MAGIC   oh._source
