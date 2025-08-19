# Databricks notebook source
# MAGIC %sql
# MAGIC create database if not exists  g_apac_sm 

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW g_apac_sm.sm_products_v
# MAGIC AS
# MAGIC select
# MAGIC product_agg._ID,
# MAGIC product_agg.ansStdUom,
# MAGIC product_agg.ansStdUomConv,
# MAGIC product_agg.brandFamily,
# MAGIC product_agg.brandStrategy,
# MAGIC product_agg.dimensionUomCode,
# MAGIC product_agg.gbu,
# MAGIC product_agg.itemId,
# MAGIC product_agg.itemType,
# MAGIC product_agg.legacyAspn,
# MAGIC product_agg.lowestShippableUom,
# MAGIC product_agg.marketingCode,
# MAGIC product_agg.mrpn,
# MAGIC product_agg.name,
# MAGIC product_agg.originId as originCode,
# MAGIC product_agg.originDescription,
# MAGIC product_agg.piecesInAltCase,
# MAGIC product_agg.piecesInBag,
# MAGIC product_agg.piecesInBox,
# MAGIC product_agg.piecesInBundle,
# MAGIC product_agg.piecesInCarton,
# MAGIC product_agg.piecesInCase,
# MAGIC --product_agg.piecesinDispenserDisplay,
# MAGIC product_agg.piecesInDisplayDispenser,
# MAGIC -- primaryuomconv / piecesindisplaydispenser -- tbd
# MAGIC product_agg.piecesInDozen,
# MAGIC product_agg.piecesInDrum,
# MAGIC product_agg.piecesInEach,
# MAGIC product_agg.piecesInGross,
# MAGIC product_agg.piecesInKit,
# MAGIC product_agg.piecesInPack,
# MAGIC product_agg.piecesInRoll,
# MAGIC product_agg.piecesInRtlPack,
# MAGIC product_agg.primarySellingUom,
# MAGIC product_agg.primaryTechnology,
# MAGIC product_agg.productBrand,
# MAGIC product_agg.productBrandType,
# MAGIC product_agg.productCategory,
# MAGIC product_agg.productCode,
# MAGIC product_agg.productDivision,
# MAGIC product_agg.productFamily,
# MAGIC product_agg.productGroup,
# MAGIC product_agg.productLegacy,
# MAGIC product_agg.productM4Category,
# MAGIC product_agg.productM4Family,
# MAGIC product_agg.productM4Group,
# MAGIC product_agg.productM4Segment,
# MAGIC product_agg.productMaterialSubType,
# MAGIC product_agg.productMaterialType,
# MAGIC product_agg.productNps,
# MAGIC product_agg.productSbu,
# MAGIC product_agg.productStatus,
# MAGIC product_agg.productStyle,
# MAGIC product_agg.productStyleDescription,
# MAGIC product_agg.productSubBrand,
# MAGIC product_agg.productSubCategory,
# MAGIC product_agg.productTransferPrice,
# MAGIC product_agg.productType,
# MAGIC product_agg.productVariant,
# MAGIC product_agg.styleItem,
# MAGIC product_agg.productStatusEffectiveDate,
# MAGIC product_agg.primaryUomConv,
# MAGIC product_agg.secondarySellingUom,
# MAGIC product_agg.productStatusImplementedDate,
# MAGIC product_agg.productStatusChangeDate,
# MAGIC product_agg.launchDate,
# MAGIC product_agg.caseLength,
# MAGIC product_agg.caseWidth,
# MAGIC product_agg.caseHeight,
# MAGIC product_agg.shelfLife,
# MAGIC product_agg.caseVolume,
# MAGIC product_agg.volumeUomCode,
# MAGIC product_agg.caseNetWeight,
# MAGIC product_agg.weightUomCode,
# MAGIC product_origin_agg.commoditycode,
# MAGIC product_origin_agg.primaryorigin
# MAGIC from
# MAGIC   s_core.product_agg
# MAGIC   left join (select productcode,  originid, primaryorigin, max(product_origin_agg.commoditycode) commoditycode
# MAGIC              from s_core.product_origin_agg where _SOURCE = 'PDH' and not _deleted
# MAGIC              group by productcode,  originid, primaryorigin ) product_origin_agg on
# MAGIC     product_agg.productcode = product_origin_agg.productcode   and product_agg.originId = product_origin_agg.originid
# MAGIC where
# MAGIC   product_agg._SOURCE = 'EBS'
# MAGIC   AND not product_agg._DELETED
# MAGIC   AND itemType = 'FINISHED GOODS'

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW g_apac_sm.sm_products_org_v
# MAGIC as
# MAGIC select
# MAGIC   item_ID,
# MAGIC organization_ID,
# MAGIC   productStatus
# MAGIC from s_core.product_org_agg
# MAGIC   where _SOURCE = 'EBS'
# MAGIC   AND not _DELETED

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW g_apac_sm.sm_operating_units_v
# MAGIC AS SELECT
# MAGIC _ID,
# MAGIC name,
# MAGIC organizationCode,
# MAGIC organizationId
# MAGIC from s_core.organization_agg
# MAGIC WHERE _SOURCE = 'EBS'
# MAGIC AND organizationType = 'OPERATING_UNIT'
# MAGIC AND not _DELETED;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW g_apac_sm.sm_inventory_units_v
# MAGIC AS SELECT
# MAGIC   _ID,
# MAGIC   name,
# MAGIC   organizationCode,
# MAGIC   organizationId,
# MAGIC   case when organizationCode = '325'
# MAGIC     then 'Drop Shipmnent'
# MAGIC     else 'Warehouse Shipment'
# MAGIC   end dropShipmentFlag,
# MAGIC   '#1167' addressLine1,
# MAGIC   '#1167' addressLine2,
# MAGIC   '#1167' city,
# MAGIC   '#1167' postalCode,
# MAGIC   '#1167' country,
# MAGIC   '#1167' state
# MAGIC from s_core.organization_agg
# MAGIC WHERE _SOURCE = 'EBS'
# MAGIC AND organizationType = 'INV'
# MAGIC AND not _DELETED;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW g_apac_sm.sm_accounts_v AS
# MAGIC select
# MAGIC   _ID,
# MAGIC   accountId,
# MAGIC   accountNumber,
# MAGIC   name,
# MAGIC   customerType
# MAGIC from
# MAGIC   s_core.account_agg
# MAGIC where
# MAGIC   account_agg._source = 'EBS'
# MAGIC   --and customerType = 'External'
# MAGIC   and not account_agg._deleted;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW g_apac_sm.sm_origin_v AS
# MAGIC select
# MAGIC   _ID,
# MAGIC   ansellPlant,
# MAGIC   originId,
# MAGIC   originName
# MAGIC from
# MAGIC   s_core.origin_agg
# MAGIC where
# MAGIC   origin_agg._source = 'EBS'
# MAGIC   and not _deleted;  

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace view g_apac_sm.sm_sales_orders_v as
# MAGIC select
# MAGIC   ol.salesorderdetailid,
# MAGIC   ol._ID salesOrderDetail_ID,
# MAGIC   oh.orderDate,
# MAGIC   year(oh.orderDate) orderedYear,
# MAGIC   year(oh.orderDate) || ' / ' || month(oh.orderDate) OrderedMonth,
# MAGIC   int(date_format(oh.orderDate, 'yyyyMMdd') ) orderDateId,
# MAGIC   ol.requestDeliveryBy,
# MAGIC   ol.scheduledShipDate,
# MAGIC   year(ol.requestDeliveryBy) requestYear,
# MAGIC   year(ol.requestDeliveryBy) || ' / ' || month(ol.requestDeliveryBy) RequestMonth,
# MAGIC   case when ol.orderLineStatus in ('CLOSED', 'CANCELLED')
# MAGIC                       then ol.orderLineStatus
# MAGIC             when ol.orderLineHoldType is not null or oh.orderHoldType is not null
# MAGIC                       then 'On Hold'
# MAGIC             else ol.orderLineStatus
# MAGIC   end  orderLineStatus,
# MAGIC   case when ol.orderStatusDetail in ('Closed', 'Cancelled')
# MAGIC                       then ol.orderStatusDetail
# MAGIC             when ol.orderLineHoldType is not null or oh.orderHoldType is not null
# MAGIC                       then 'On Hold'
# MAGIC             else ol.orderStatusDetail
# MAGIC   end  orderStatusDetail,
# MAGIC   orderLineStatus originalOrderLineStatus,
# MAGIC   oh.ordernumber,
# MAGIC   ol.customerPoNumber,
# MAGIC   ol.lineNumber,
# MAGIC   ol.sequenceNumber,
# MAGIC   ol.returnReasonCode ,
# MAGIC   round(ol.returnAmount, 2) returnAmount,
# MAGIC   round(ol.returnAmount / exchangeRateUsd,2) returnAmountUsd,
# MAGIC   round(ol.quantityReturned *  ol.primaryUomConv,2) quantityReturned,
# MAGIC   round(ol.backOrderValue, 2) backOrderValue,
# MAGIC   round(ol.backOrderValue / oh.exchangeRateUsd,2) backOrderValueUsd,
# MAGIC   round(ol.quantityBackOrdered , 2) quantityBackOrdered,
# MAGIC   round(ol.quantityOrdered, 2) quantityOrdered,
# MAGIC   round(ol.quantityReserved, 2) quantityReserved ,
# MAGIC   round(ol.quantityCancelled, 2) quantityCancelled,
# MAGIC   round(ol.quantityReserved  * ol.pricePerUnit, 2) reservedAmount,
# MAGIC   round(ol.quantityReserved  * ol.pricePerUnit / exchangeRateUsd,2) reservedAmountUsd,
# MAGIC   round(ol.quantityOrdered * ol.caseUomConv, 2) quantityOrderedCase,
# MAGIC   round(ol.quantityOrdered * ol.ansStdUomConv, 2) quantityOrderedAnsStduom,
# MAGIC   round(ol.orderAmount, 2) orderAmount,
# MAGIC   round(ol.quantityBackOrdered * ol.caseUomConv, 2) quantityBackOrderedCase,
# MAGIC   round(ol.quantityBackOrdered * ol.ansStdUomConv, 2) quantityBackOrderedAnsStdUom,
# MAGIC   ol.pricePerUnit, 
# MAGIC   round(ol.orderAmount /  oh.exchangeRateUsd,2) orderAmountUsd, 
# MAGIC   '' apacScheduledShipDate, -- ol.apacScheduledShipDate,
# MAGIC   ol.orderUomCode,
# MAGIC   round(ol.pricePerUnit * ol.quantityCancelled, 2) cancelledAmount,
# MAGIC   round(ol.pricePerUnit * ol.quantityCancelled /  oh.exchangeRateUsd, 2) cancelledAmountUsd,
# MAGIC   oh.transactionCurrencyId,
# MAGIC   oh.orderHeaderStatus,
# MAGIC   oh.customer_ID,
# MAGIC   oh.orderType_ID,
# MAGIC   oh.owningBusinessUnit_ID operatingUnit_ID,
# MAGIC   ol.inventoryWarehouse_ID inventoryUnit_ID,
# MAGIC   ol.item_id product_ID,
# MAGIC   oh.createdBy_ID headerCreatedBy_ID,
# MAGIC   ol.createdBy_ID lineCreatedBy_ID,
# MAGIC   oh.modifiedBy_ID headerModifiedBy_ID,
# MAGIC   ol.modifiedBy_ID lineModifiedBy_ID,
# MAGIC   oh.party_ID,
# MAGIC   oh.customer_ID account_ID,
# MAGIC   case when ol.deliveryNoteId is null then 0 
# MAGIC     else ol.quantityOrdered
# MAGIC   end quantityPickSlip,
# MAGIC   case when ol.deliveryNoteId is null then 0 
# MAGIC     else round(ol.quantityOrdered * ol.caseUomConv, 2)
# MAGIC   end quantityPickSlipCase,
# MAGIC   case when ol.deliveryNoteId is null then 0 
# MAGIC     else round(ol.quantityOrdered * ol.ansStdUomConv, 2)
# MAGIC   end quantityPickSlipAnsStdUom,
# MAGIC   case when ol.deliveryNoteId is null then 0 
# MAGIC     else (ol.orderAmount )
# MAGIC   end pickSlipAmount,
# MAGIC   case when ol.deliveryNoteId is null then 0 
# MAGIC     else round(ol.orderAmount /  oh.exchangeRateUsd,2)
# MAGIC   end pickSlipAmountUsd,
# MAGIC   case
# MAGIC     when ol.quantityReserved = 0 and ol.deliveryNoteId is null and apacScheduledShipDate <> ''
# MAGIC       then (ol.orderAmount  )
# MAGIC    end notAllocatedScheduledAmount,
# MAGIC   case
# MAGIC     when ol.quantityReserved = 0 and ol.deliveryNoteId is null and apacScheduledShipDate <> ''
# MAGIC       then  round(ol.orderAmount / oh.exchangeRateUsd,2)
# MAGIC    end notAllocatedScheduledAmountUsd,
# MAGIC   case
# MAGIC     when ol.quantityReserved = 0 and ol.deliveryNoteId is null and apacScheduledShipDate <> ''
# MAGIC       then ol.quantityOrdered
# MAGIC    end quantityNotAllocatedScheduled,
# MAGIC   case
# MAGIC     when ol.quantityReserved = 0 and ol.deliveryNoteId is null and apacScheduledShipDate <> ''
# MAGIC       then round(ol.quantityOrdered * ol.caseUomConv, 2)
# MAGIC    end quantityNotAllocatedScheduledCase,
# MAGIC   case
# MAGIC     when ol.quantityReserved = 0 and ol.deliveryNoteId is null and apacScheduledShipDate <> ''
# MAGIC       then round(ol.quantityOrdered * ol.ansStdUomConv, 2)
# MAGIC    end quantityNotAllocatedScheduledAnsStdUom,
# MAGIC   case
# MAGIC     when ol.quantityReserved = 0 and ol.deliveryNoteId is null and apacScheduledShipDate = ''
# MAGIC       then ol.orderAmount 
# MAGIC    end notAllocatedNotScheduledAmount,
# MAGIC    case
# MAGIC     when ol.quantityReserved = 0 and ol.deliveryNoteId is null and apacScheduledShipDate = ''
# MAGIC       then  round(ol.orderAmount / oh.exchangeRateUsd,2)
# MAGIC    end notAllocatedNotScheduledAmountUsd,
# MAGIC    case
# MAGIC     when ol.quantityReserved = 0 and ol.deliveryNoteId is null and apacScheduledShipDate = ''
# MAGIC       then ol.quantityOrdered
# MAGIC    end quantityNotAllocatedNotScheduled,
# MAGIC    case
# MAGIC     when ol.quantityReserved = 0 and ol.deliveryNoteId is null and apacScheduledShipDate = ''
# MAGIC       then round(ol.quantityOrdered * ol.caseUomConv, 2)
# MAGIC    end quantityNotAllocatedNotScheduledCase,
# MAGIC    case
# MAGIC     when ol.quantityReserved = 0 and ol.deliveryNoteId is null and apacScheduledShipDate = ''
# MAGIC       then round(ol.quantityOrdered * ol.ansStdUomConv, 2)
# MAGIC    end quantityNotAllocatedNotScheduledAnsStdUom,
# MAGIC    case when oh.orderHoldType in ('Ansell Credit Check Failure', 'Credit Card Auth Failure', 'Credit Check Failure')
# MAGIC        then 'Credit Hold'
# MAGIC    end  creditHoldFlag,
# MAGIC    oh.orderHoldType,
# MAGIC    ol.orderLineHoldType,
# MAGIC    ol.supplierNumber,
# MAGIC    ol.supplierName,
# MAGIC    ol.shipToAddress_ID,
# MAGIC    oh.billToAddress_ID,
# MAGIC    ol.deliveryNoteId,
# MAGIC    ol.actualShipDate,
# MAGIC    oh.orderHoldDate1,
# MAGIC    oh.orderHoldDate2,
# MAGIC    ol.orderLineHoldDate1,
# MAGIC    ol.orderLineHoldDate2,
# MAGIC    ol.quantityInvoiced,
# MAGIC    round(ol.quantityInvoiced * pricePerUnit , 2) invoicedAmount,
# MAGIC    oh.baseCurrencyId,
# MAGIC    oh.paymentTerm_ID,
# MAGIC    ol.itemIdentifierType,
# MAGIC    ol._modified,
# MAGIC    ol.primaryUomCode,
# MAGIC    ol.dropShipPoNumber,
# MAGIC    ol.orderedItem
# MAGIC from
# MAGIC   s_supplychain.sales_order_headers_agg oh
# MAGIC   join s_supplychain.sales_order_lines_agg ol on ol.salesorder_id = oh._ID
# MAGIC   join s_core.TRANSACTION_TYPE_AGG ot on oh.orderType_ID = ot._ID
# MAGIC where oh._source = 'EBS'
# MAGIC and not OL._DELETED
# MAGIC and not OH._DELETED
# MAGIC and not ot._DELETED

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace view g_apac_sm.sm_sales_backorders_v as
# MAGIC select
# MAGIC   oh.orderDate,
# MAGIC   int(date_format(oh.orderDate, 'yyyyMMdd') ) orderDateId,
# MAGIC   ol.requestDeliveryBy,
# MAGIC   case when ol.orderLineStatus in ('CLOSED', 'CANCELLED')
# MAGIC                       then ol.orderLineStatus
# MAGIC             when ol.orderLineHoldType is not null or oh.orderHoldType is not null
# MAGIC                       then 'On Hold'
# MAGIC             else ol.orderLineStatus
# MAGIC   end  orderLineStatus,
# MAGIC   case when ol.orderStatusDetail in ('Closed', 'Cancelled')
# MAGIC                       then ol.orderStatusDetail
# MAGIC             when ol.orderLineHoldType is not null or oh.orderHoldType is not null
# MAGIC                       then 'On Hold'
# MAGIC             else ol.orderStatusDetail
# MAGIC   end  orderStatusDetail,
# MAGIC   oh.ordernumber,
# MAGIC   ol.lineNumber,
# MAGIC   ol.returnReasonCode ,
# MAGIC   ol.returnAmount,
# MAGIC   round(ol.returnAmount / exchangeSpotRate,2) returnAmountUsd,
# MAGIC   round(ol.quantityReturned *  ol.primaryUomConv,2) quantityReturned,
# MAGIC   ol.backOrderValue,
# MAGIC   round(ol.backOrderValue / oh.exchangeSpotRate,2) backOrderValueUsd,
# MAGIC   ol.quantityBackOrdered,
# MAGIC   round(ol.quantityBackOrdered * ol.caseUomConv, 2) quantityBackOrderedCase,
# MAGIC   round(ol.quantityBackOrdered * ol.ansStdUomConv, 2) quantityBackOrderedAnsStdUom,
# MAGIC   oh.customer_ID,
# MAGIC   oh.orderType_ID,
# MAGIC   oh.owningBusinessUnit_ID operatingUnit_ID,
# MAGIC   ol.inventoryWarehouse_ID inventoryUnit_ID,
# MAGIC   ol.item_id product_ID,
# MAGIC   oh.createdBy_ID,
# MAGIC   oh.party_ID,
# MAGIC   oh.customer_ID account_ID,
# MAGIC   oh.billtoAddress_ID,
# MAGIC   oh.shipToAddress_ID,
# MAGIC   ol.item_ID
# MAGIC from
# MAGIC   s_supplychain.sales_order_headers_agg oh
# MAGIC   join s_supplychain.sales_order_lines_agg ol on ol.salesorder_id = oh._ID
# MAGIC   join s_core.TRANSACTION_TYPE_AGG ot on oh.orderType_ID = ot._ID
# MAGIC   join s_core.account_agg ac on oh.customer_ID = ac._ID
# MAGIC   join s_core.organization_agg inv on ol.inventoryWarehouse_ID = inv._ID
# MAGIC where oh._source = 'EBS'
# MAGIC and ol.bookedFlag = 'Y'
# MAGIC and not OL._DELETED
# MAGIC and not OH._DELETED
# MAGIC and not ot._DELETED
# MAGIC and ol.backOrderValue <> 0
# MAGIC and ol.requestDeliveryBy < current_date
# MAGIC and ol.orderStatusDetail not in ('Cancelled', 'Order Cancelled', 'Pending pre-billing acceptance', 'Closed')
# MAGIC and ac.customerType = 'External'
# MAGIC and ol.orderLineHoldType is  null -- tbd whether orders on hold should be incluced
# MAGIC and oh.orderHoldType is  null -- tbd whether orders on hold should be incluced
# MAGIC and inv.organizationCode in ('325', '326', '327', '724')
# MAGIC and ot.name in ('ASIA_Broken Case Order', 'ASIA_Broken Case Order Line', 'ASIA_Donation Order', 'ASIA_Donation Order Line', 'ASIA_Drop Shipment Order', 'ASIA_Drop Shipment Order Line', 'ASIA_Internal Line', 'ASIA_Internal Order', 'ASIA_Manual PriceAllowed Line', 'ASIA_Manual PriceAllowed Order', 'ASIA_Prepayment Order', 'ASIA_Prepayment Order Line', 'ASIA_Price related CN', 'ASIA_Price related DN', 'ASIA_Price related_CN Line', 'ASIA_Price related_CN Line1', 'ASIA_Price related_DN Line', 'ASIA_Return CN_No Inv Line', 'ASIA_Return CN_No Inv Line1', 'ASIA_Return DN_No Inv Line', 'ASIA_Return Order', 'ASIA_Return Order CN_No Inv', 'ASIA_Return Order DN_No Inv', 'ASIA_Return Order Line', 'ASIA_Return Order Line1', 'ASIA_Sample Drop Ship Order', 'ASIA_Sample Drop Shipment Line', 'ASIA_Sample Order Line', 'ASIA_Samples Order', 'ASIA_Service Order CN', 'ASIA_Service Order DN', 'ASIA_Service Order_CN Line', 'ASIA_Service Order_CN Line1', 'ASIA_Service Order_DN Line', 'ASIA_Standard Order', 'ASIA_Standard Order Line', 'IN_PP_Standard_Order', 'IN_PP_Drop Shipment Order')

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW g_apac_sm.sm_sales_invoices_v AS 
# MAGIC select 
# MAGIC   invh.owningBusinessUnit_ID,
# MAGIC   invh.customer_ID,
# MAGIC   invl.item_ID,
# MAGIC   invh.party_ID,
# MAGIC   invh.billtoAddress_ID,
# MAGIC   invh.shiptoAddress_ID,
# MAGIC   invh.transaction_ID,
# MAGIC   invl.orderNumber,
# MAGIC   invh.dateInvoiced,
# MAGIC   year(invh.dateInvoiced) yearInvoiced,
# MAGIC   year(invh.dateInvoiced) || ' / ' || month(invh.dateInvoiced) yearMonthInvoiced,
# MAGIC   invh.invoiceNumber,
# MAGIC   round(sum(invl.baseAmount),2) baseAmount,
# MAGIC   round(sum(invl.baseAmount * invh.exchangeRate),2) baseAmountLE,
# MAGIC   round(sum(invl.baseAmount / invh.exchangeSpotRate),2) baseAmountUsd,
# MAGIC   round(sum(invl.baseAmount + invl.returnAmount),2) grossAmount,
# MAGIC   round(sum((invl.baseAmount + invl.returnAmount) * invh.exchangeRate),2) grossAmountLE,
# MAGIC   round(sum((invl.baseAmount + invl.returnAmount) / invh.exchangeSpotRate),2) grossAmountUsd,
# MAGIC   round(sum(invl.quantityShipped * invl.caseUomConv),0) quantityShippedCase,
# MAGIC   round(sum(invl.quantityShipped * invl.ansStdUomConv),0) quantityShippedAnsStdUom,
# MAGIC   round(sum(invl.returnAmount),2) returnAmount,
# MAGIC   round(sum(invl.returnAmount * invh.exchangeRate),2) returnAmountLE,
# MAGIC   round(sum(invl.returnAmount / invh.exchangeSpotRate),2) returnAmountUsd,
# MAGIC   round(sum(invl.quantityInvoiced),2) quantityInvoiced ,
# MAGIC   round(sum(invl.quantityInvoiced * invl.caseUomConv),2) quantityInvoicedCase ,
# MAGIC   round(sum(invl.quantityInvoiced * invl.ansStdUomConv),2) quantityInvoicedAnsStdUom ,
# MAGIC   invl.customerPONumber,
# MAGIC   invh.baseCurrencyId,
# MAGIC   invh.transactionCurrencyId,
# MAGIC   invl.inventoryWarehouse_ID,
# MAGIC   invh.owningBusinessUnit_ID operatingUnit_ID,
# MAGIC   invl.salesorderdetail_ID,
# MAGIC   invl.salesOrderDetailid,
# MAGIC   invl.priceListId,
# MAGIC   invl.priceListName,
# MAGIC   invl.scheduledShipDate
# MAGIC from 
# MAGIC   s_supplychain.sales_invoice_headers_agg invh
# MAGIC   join s_supplychain.sales_invoice_lines_agg invl on invh._ID = invl.invoice_ID
# MAGIC   join s_core.account_agg ac on invh.customer_ID = ac._ID
# MAGIC where
# MAGIC   not invh._deleted
# MAGIC   and not invl._deleted
# MAGIC   and invh._source = 'EBS'
# MAGIC   and invl._source = 'EBS'
# MAGIC   and ac.customerType = 'External'
# MAGIC   and salesOrderDetailid is not null
# MAGIC group by 
# MAGIC   invh.owningBusinessUnit_ID,
# MAGIC   invh.customer_ID,
# MAGIC   invh.billtoAddress_ID,
# MAGIC   invh.shiptoAddress_ID,
# MAGIC   invh.dateInvoiced,
# MAGIC   invh.invoiceNumber,
# MAGIC   invl.customerPONumber,
# MAGIC   invh.baseCurrencyId,
# MAGIC   invl.item_ID,
# MAGIC   invh.party_ID,
# MAGIC   invh.transaction_ID,
# MAGIC   invh.transactionCurrencyId,
# MAGIC   invl.orderNumber,
# MAGIC   invl.inventoryWarehouse_ID,
# MAGIC   invl.salesorderdetail_ID,
# MAGIC   invl.salesOrderDetailid,
# MAGIC   invl.priceListId,
# MAGIC   invl.priceListName,
# MAGIC   invl.scheduledShipDate

# COMMAND ----------

# MAGIC %sql
# MAGIC  create or replace view g_apac_sm.sm_sales_shipping_v as
# MAGIC  select 
# MAGIC    sales_order_headers_agg.ordernumber,
# MAGIC    sales_order_lines_agg.salesorderdetailid,
# MAGIC    sales_order_lines_agg.promisedOnDate,
# MAGIC    sales_order_lines_agg.requestDeliveryBy,
# MAGIC    sales_shipping_lines_agg.actualShipDate,
# MAGIC    sales_shipping_lines_agg.actualPickDate,
# MAGIC    int(date_format(sales_shipping_lines_agg.actualPickDate, 'yyyyMMdd')) pickDateId,
# MAGIC    sales_order_lines_agg.apacScheduledShipDate,
# MAGIC    sales_order_headers_agg.orderDate,
# MAGIC    sales_shipping_lines_agg.lineNumber,
# MAGIC    sales_shipping_lines_agg.sequenceNumber,
# MAGIC    sales_shipping_lines_agg.lotCount,
# MAGIC    sales_shipping_lines_agg.quantityOrdered * sales_shipping_lines_agg.primaryUomConv quantityOrderedPrimaryUom,
# MAGIC    sales_shipping_lines_agg.quantityShipped * sales_shipping_lines_agg.primaryUomConv quantityShippedPrimaryUom,
# MAGIC    sales_shipping_lines_agg.quantityActualShippedOntime1 * sales_shipping_lines_agg.primaryUomConv quantityActualShippedOntime1PrimaryUom,
# MAGIC    sales_shipping_lines_agg.quantityActualShippedOntime2 * sales_shipping_lines_agg.primaryUomConv quantityActualShippedOntime2PrimaryUom,
# MAGIC    sales_shipping_lines_agg.quantityActualShippedOntime3 * sales_shipping_lines_agg.primaryUomConv quantityActualShippedOntime3PrimaryUom,
# MAGIC    sales_shipping_lines_agg.quantityActualShippedOntime4 * sales_shipping_lines_agg.primaryUomConv quantityActualShippedOntime4PrimaryUom,
# MAGIC    sales_shipping_lines_agg.quantityActualShippedOntime5 * sales_shipping_lines_agg.primaryUomConv quantityActualShippedOntime5PrimaryUom,
# MAGIC    sales_shipping_lines_agg.quantityActualShippedOntime6 * sales_shipping_lines_agg.primaryUomConv quantityActualShippedOntime6PrimaryUom,
# MAGIC    sales_shipping_lines_agg.quantityActualShippedOntime7 * sales_shipping_lines_agg.primaryUomConv quantityActualShippedOntime7PrimaryUom,
# MAGIC    sales_shipping_lines_agg.quantityActualShippedOntime8 * sales_shipping_lines_agg.primaryUomConv quantityActualShippedOntime8PrimaryUom,
# MAGIC    sales_shipping_lines_agg.quantityActualShippedOntime9 * sales_shipping_lines_agg.primaryUomConv quantityActualShippedOntime9PrimaryUom,
# MAGIC    sales_shipping_lines_agg.quantityActualShippedOntime10 * sales_shipping_lines_agg.primaryUomConv quantityActualShippedOntime10PrimaryUom,
# MAGIC    sales_shipping_lines_agg.quantityActualShippedOntimePromisedDate1 * sales_shipping_lines_agg.primaryUomConv quantityActualShippedOntimePromisedDate1PrimaryUom,
# MAGIC    sales_shipping_lines_agg.quantityActualShippedOntimePromisedDate2 * sales_shipping_lines_agg.primaryUomConv quantityActualShippedOntimePromisedDate2PrimaryUom,
# MAGIC    sales_shipping_lines_agg.quantityActualShippedOntimePromisedDate3 * sales_shipping_lines_agg.primaryUomConv quantityActualShippedOntimePromisedDate3PrimaryUom,
# MAGIC    sales_shipping_lines_agg.quantityActualShippedOntimePromisedDate4 * sales_shipping_lines_agg.primaryUomConv quantityActualShippedOntimePromisedDate4PrimaryUom,
# MAGIC    sales_shipping_lines_agg.quantityActualShippedOntimePromisedDate5 * sales_shipping_lines_agg.primaryUomConv quantityActualShippedOntimePromisedDate5PrimaryUom,
# MAGIC    sales_shipping_lines_agg.quantityActualShippedOntimePromisedDate6 * sales_shipping_lines_agg.primaryUomConv quantityActualShippedOntimePromisedDate6PrimaryUom,
# MAGIC    sales_shipping_lines_agg.quantityActualShippedOntimePromisedDate7 * sales_shipping_lines_agg.primaryUomConv quantityActualShippedOntimePromisedDate7PrimaryUom,
# MAGIC    sales_shipping_lines_agg.quantityActualShippedOntimePromisedDate8 * sales_shipping_lines_agg.primaryUomConv quantityActualShippedOntimePromisedDate8PrimaryUom,
# MAGIC    sales_shipping_lines_agg.quantityActualShippedOntimePromisedDate9 * sales_shipping_lines_agg.primaryUomConv quantityActualShippedOntimePromisedDate9PrimaryUom,
# MAGIC    sales_shipping_lines_agg.quantityActualShippedOntimePromisedDate10 * sales_shipping_lines_agg.primaryUomConv quantityActualShippedOntimePromisedDate10PrimaryUom,
# MAGIC    sales_shipping_lines_agg.shippedOntime1Indicator,
# MAGIC    sales_shipping_lines_agg.shippedOntime2Indicator,
# MAGIC    sales_shipping_lines_agg.shippedOntime3Indicator,
# MAGIC    sales_shipping_lines_agg.shippedOntime4Indicator,
# MAGIC    sales_shipping_lines_agg.shippedOntime5Indicator,
# MAGIC    sales_shipping_lines_agg.shippedOntime6Indicator,
# MAGIC    sales_shipping_lines_agg.shippedOntime7Indicator,
# MAGIC    sales_shipping_lines_agg.shippedOntime8Indicator,
# MAGIC    sales_shipping_lines_agg.shippedOntime9Indicator,
# MAGIC    sales_shipping_lines_agg.shippedOntime10Indicator,
# MAGIC    sales_shipping_lines_agg.shippedOntime1IndicatorPromisedDate,
# MAGIC    sales_shipping_lines_agg.shippedOntime2IndicatorPromisedDate,
# MAGIC    sales_shipping_lines_agg.shippedOntime3IndicatorPromisedDate,
# MAGIC    sales_shipping_lines_agg.shippedOntime4IndicatorPromisedDate,
# MAGIC    sales_shipping_lines_agg.shippedOntime5IndicatorPromisedDate,
# MAGIC    sales_shipping_lines_agg.shippedOntime6IndicatorPromisedDate,
# MAGIC    sales_shipping_lines_agg.shippedOntime7IndicatorPromisedDate,
# MAGIC    sales_shipping_lines_agg.shippedOntime8IndicatorPromisedDate,
# MAGIC    sales_shipping_lines_agg.shippedOntime9IndicatorPromisedDate,
# MAGIC    sales_shipping_lines_agg.shippedOntime10IndicatorPromisedDate,
# MAGIC    sales_shipping_lines_agg.primaryUomConv,
# MAGIC    sales_shipping_lines_agg.inventoryWarehouse_ID,
# MAGIC    s_supplychain.sales_order_headers_agg.owningBusinessUnit_ID,
# MAGIC    sales_order_headers_agg.shipToAddress_ID,
# MAGIC    sales_order_headers_agg.orderType_ID,
# MAGIC    sales_order_headers_agg.customer_ID,
# MAGIC    sales_order_lines_agg.item_ID,
# MAGIC    sales_shipping_lines_agg.deliveryDetailId
# MAGIC  from s_supplychain.sales_shipping_lines_agg
# MAGIC  join s_supplychain.sales_order_headers_agg on sales_shipping_lines_agg.salesorder_id = sales_order_headers_agg._ID
# MAGIC  join s_supplychain.sales_order_lines_agg on sales_shipping_lines_agg.salesOrderDetail_ID = sales_order_lines_agg._ID 
# MAGIC  
# MAGIC  where 
# MAGIC    sales_order_headers_agg._source = 'EBS'
# MAGIC    and sales_shipping_lines_agg._source = 'EBS'
# MAGIC    and sales_order_lines_agg._source = 'EBS'
# MAGIC    and not sales_order_headers_agg._deleted
# MAGIC    and not sales_shipping_lines_agg._deleted
# MAGIC    and not sales_order_lines_agg._deleted

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW g_apac_sm.sm_customer_location_v AS
# MAGIC select
# MAGIC   _ID,
# MAGIC   customer_location_agg.accountID,
# MAGIC   customer_location_agg.accountNumber,
# MAGIC   customer_location_agg.addressId,
# MAGIC   customer_location_agg.addressLine1,
# MAGIC   customer_location_agg.addressLine2,
# MAGIC   customer_location_agg.addressLine3,
# MAGIC   customer_location_agg.addressLine4,
# MAGIC   customer_location_agg.city,
# MAGIC   customer_location_agg.country,
# MAGIC   customer_location_agg.emailAddress,
# MAGIC   customer_location_agg.mdmId,
# MAGIC   customer_location_agg.name,
# MAGIC   customer_location_agg.partySiteNumber,
# MAGIC   customer_location_agg.phoneNumber,
# MAGIC   customer_location_agg.postalCode,
# MAGIC   customer_location_agg.siteCategory,
# MAGIC   customer_location_agg.siteUseCode,
# MAGIC   customer_location_agg.state,
# MAGIC   customer_location_agg.territory_ID
# MAGIC from
# MAGIC   s_core.customer_location_agg
# MAGIC where
# MAGIC   customer_location_agg._source = 'EBS'
# MAGIC   and not customer_location_agg._deleted

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW g_apac_sm.sm_order_types_v AS
# MAGIC select
# MAGIC   _ID,
# MAGIC   description,
# MAGIC   name,
# MAGIC   transactionId,
# MAGIC   transactionTypeCode,
# MAGIC   transactionTypeGroup
# MAGIC from
# MAGIC   s_core.transaction_type_agg
# MAGIC where
# MAGIC   transaction_type_agg._source = 'EBS'
# MAGIC   and transaction_type_agg.transactionGroup = 'ORDER_TYPE'
# MAGIC   and not transaction_type_agg._deleted

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW g_apac_sm.sm_transaction_types_v AS
# MAGIC select
# MAGIC   _ID,
# MAGIC   description,
# MAGIC   name,
# MAGIC   transactionId,
# MAGIC   transactionTypeCode
# MAGIC from
# MAGIC   s_core.transaction_type_agg
# MAGIC where
# MAGIC   transaction_type_agg._source = 'EBS'
# MAGIC   and transaction_type_agg.transactionGroup = 'INVOICE_TYPE'
# MAGIC   and not transaction_type_agg._deleted

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW g_apac_sm.sm_payment_terms_v AS
# MAGIC select
# MAGIC   _ID,
# MAGIC   paymentTermName
# MAGIC from
# MAGIC   s_core.payment_terms_agg
# MAGIC where
# MAGIC   payment_terms_agg._source = 'EBS'
# MAGIC   and not payment_terms_agg._deleted

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW g_apac_sm.sm_territory_v AS
# MAGIC select
# MAGIC   _ID,
# MAGIC   territoryCode,
# MAGIC   territoryShortName,
# MAGIC   territoryDescription,
# MAGIC   euCode,
# MAGIC   obsoleteFlag,
# MAGIC   isoCodeNumeric,
# MAGIC   isoCode,
# MAGIC   nlsCode
# MAGIC from
# MAGIC   s_core.territory_agg
# MAGIC where
# MAGIC   territory_agg._source = 'EBS'
# MAGIC   and not territory_agg._deleted
