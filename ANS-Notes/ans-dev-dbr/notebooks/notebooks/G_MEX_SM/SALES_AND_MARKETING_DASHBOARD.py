# Databricks notebook source
# MAGIC %sql
# MAGIC Create database if not exists g_mex_sm;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW g_mex_sm.sm_products_v
# MAGIC AS select
# MAGIC _ID,
# MAGIC ansStdUom,
# MAGIC ansStdUomConv,
# MAGIC brandFamily,
# MAGIC brandStrategy,
# MAGIC gbu,
# MAGIC itemId,
# MAGIC itemType,
# MAGIC legacyAspn,
# MAGIC lowestShippableUom,
# MAGIC marketingCode,
# MAGIC mrpn,
# MAGIC name,
# MAGIC originId as originCode,
# MAGIC originDescription,
# MAGIC piecesInAltCase,
# MAGIC piecesInBag,
# MAGIC piecesInBox,
# MAGIC piecesInBundle,
# MAGIC piecesInCarton,
# MAGIC piecesInCase,
# MAGIC -- piecesinDispenserDisplay,
# MAGIC piecesInDisplayDispenser,
# MAGIC -- primaryuomconv / piecesindisplaydispenser -- tbd
# MAGIC piecesInDozen,
# MAGIC piecesInDrum,
# MAGIC piecesInEach,
# MAGIC piecesInGross,
# MAGIC piecesInKit,
# MAGIC piecesInPack,
# MAGIC piecesInRoll,
# MAGIC piecesInRtlPack,
# MAGIC primarySellingUom,
# MAGIC primaryTechnology,
# MAGIC productBrand,
# MAGIC productBrandType,
# MAGIC productCategory,
# MAGIC productCode,
# MAGIC productDivision,
# MAGIC productFamily,
# MAGIC productGroup,
# MAGIC productLegacy,
# MAGIC productM4Category,
# MAGIC productM4Family,
# MAGIC productM4Group,
# MAGIC productM4Segment,
# MAGIC productMaterialSubType,
# MAGIC productMaterialType,
# MAGIC productNps,
# MAGIC productSbu,
# MAGIC productStatus,
# MAGIC productStyle,
# MAGIC productStyleDescription,
# MAGIC productSubBrand,
# MAGIC productSubCategory,
# MAGIC productTransferPrice,
# MAGIC productType,
# MAGIC productVariant,
# MAGIC styleItem
# MAGIC from s_core.product_agg
# MAGIC where _SOURCE = 'EBS'
# MAGIC AND not _DELETED;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW g_mex_sm.sm_product_orgs_v 
# MAGIC AS
# MAGIC select
# MAGIC   itemId,
# MAGIC   item_ID,
# MAGIC   commodityCode,
# MAGIC   organizationId,
# MAGIC   organization_ID
# MAGIC from 
# MAGIC   s_core.product_org_agg
# MAGIC where 
# MAGIC   _source = 'EBS'
# MAGIC   and not _deleted

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW g_mex_sm.sm_operating_units_v
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
# MAGIC CREATE OR REPLACE VIEW g_mex_sm.sm_inventory_units_v
# MAGIC AS SELECT
# MAGIC _ID,
# MAGIC name,
# MAGIC organizationCode,
# MAGIC organizationId
# MAGIC from s_core.organization_agg
# MAGIC WHERE _SOURCE = 'EBS'
# MAGIC AND organizationType = 'INV'
# MAGIC AND not _DELETED;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW g_mex_sm.sm_accounts_v AS
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
# MAGIC   and not account_agg._deleted;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW g_mex_sm.sm_parties_v
# MAGIC AS SELECT
# MAGIC _ID,
# MAGIC address1Line1,
# MAGIC address1Line2,
# MAGIC address1Line3,
# MAGIC mdmId,
# MAGIC partyCity,
# MAGIC partyCountry,
# MAGIC partyId,
# MAGIC partyName,
# MAGIC partyNumber,
# MAGIC partyPostalCode,
# MAGIC partyState,
# MAGIC partyType,
# MAGIC vendorId
# MAGIC from s_core.party_agg
# MAGIC WHERE _SOURCE = 'EBS'
# MAGIC AND not _DELETED;

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace view g_mex_sm.sm_users_v as
# MAGIC  select
# MAGIC firstName,
# MAGIC lastName,
# MAGIC fullName,
# MAGIC personId,
# MAGIC fullName login,
# MAGIC _ID
# MAGIC from s_core.user_agg
# MAGIC where _source = 'EBS'
# MAGIC and not _deleted ;

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace view g_mex_sm.edi_sales_orders_v AS
# MAGIC select
# MAGIC   oh.ordernumber,
# MAGIC   oh.salesorderid,
# MAGIC   ol.salesorderdetailid,
# MAGIC   oh.owningBusinessUnit_ID,
# MAGIC   ol.item_ID,
# MAGIC   oh.customer_ID,
# MAGIC   ol.inventoryWarehouse_ID,
# MAGIC   oh.orderType_ID,
# MAGIC   (case when oh.ediautobooking then oh.salesorderid end) ediautobookingorder,
# MAGIC   (case when oh.ediautobookingsuccess  and oh.ediautobooking then oh.salesorderid end) ediautobookingordersuccess,
# MAGIC   (case when oh.ediautobookingsuccess is false and oh.ediautobooking then oh.salesorderid end) ediautobookingorderfailure,
# MAGIC   oh.orderdate,
# MAGIC   (case when oh.ediautobooking and ol.ediautobooking then 1 else 0 end) ediautobookingline,
# MAGIC   (case when oh.ediautobooking and ol.eDIAutoBookingSuccess and ol.ediautobooking then 1 else 0 end)  ediautobookinglinesuccess,
# MAGIC   (case when oh.ediautobooking and ol.eDIAutoBookingSuccess is false and ol.ediautobooking then 1 else 0 end)  ediautobookinglinefailure,
# MAGIC   ol.ediAutoBookingHolds,
# MAGIC   oh.modifiedby_id headermodifiedby_id,
# MAGIC   ol.modifiedby_id linemodifiedby_id
# MAGIC from
# MAGIC   s_supplychain.sales_order_headers_agg oh
# MAGIC   join s_supplychain.sales_order_lines_agg ol on oh._ID = ol.salesorder_ID
# MAGIC   join s_core.transaction_type_agg ot on oh.orderType_ID = ot._id
# MAGIC where
# MAGIC   oh._source = 'EBS'
# MAGIC   and not oh._deleted
# MAGIC   and not ol._deleted
# MAGIC   and date_format(oh.orderdate, 'yyyy') >= '2021'
# MAGIC   and ot.transactionTypeGroup not in ('Intercompany', 'Internal Order', 'Sample', 'Safety Stock', 'Sample Return')

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace view g_mex_sm.sm_sales_orders_v as
# MAGIC select
# MAGIC   ol.salesorderdetailid,
# MAGIC   ol._ID salesOrderDetail_ID,
# MAGIC   oh.orderDate,
# MAGIC   year(oh.orderDate) orderedYear,
# MAGIC   year(oh.orderDate) || ' / ' || month(oh.orderDate) OrderedMonth,
# MAGIC   int(date_format(oh.orderDate, 'yyyyMMdd')) orderDateId,
# MAGIC   ol.requestDeliveryBy,
# MAGIC   ol.scheduledShipDate,
# MAGIC   year(ol.requestDeliveryBy) requestYear,
# MAGIC   year(ol.requestDeliveryBy) || ' / ' || month(ol.requestDeliveryBy) RequestMonth,
# MAGIC   case
# MAGIC     when ol.orderLineStatus in ('CLOSED', 'CANCELLED') then ol.orderLineStatus
# MAGIC     when ol.orderLineHoldType is not null
# MAGIC     or oh.orderHoldType is not null then 'On Hold'
# MAGIC     else ol.orderLineStatus
# MAGIC   end orderLineStatus,
# MAGIC   case
# MAGIC     when ol.orderStatusDetail in ('Closed', 'Cancelled') then ol.orderStatusDetail
# MAGIC     when ol.orderLineHoldType is not null
# MAGIC     or oh.orderHoldType is not null then 'On Hold'
# MAGIC     else ol.orderStatusDetail
# MAGIC   end orderStatusDetail,
# MAGIC   orderLineStatus originalOrderLineStatus,
# MAGIC   oh.ordernumber,
# MAGIC   ol.customerPoNumber,
# MAGIC   ol.lineNumber,
# MAGIC   ol.sequenceNumber,
# MAGIC   ol.returnReasonCode,
# MAGIC   round(ol.returnAmount, 2) returnAmount,
# MAGIC   round(ol.returnAmount / oh.exchangeSpotRate, 2) returnAmountUsd,
# MAGIC   round(ol.quantityReturned * ol.primaryUomConv, 2) quantityReturned,
# MAGIC   round(ol.backOrderValue, 2) backOrderValue,
# MAGIC   round(ol.backOrderValue / oh.exchangeSpotRate, 2) backOrderValueUsd,
# MAGIC   round(ol.quantityBackOrdered, 2) quantityBackOrdered,
# MAGIC   round(ol.quantityOrdered, 2) quantityOrdered,
# MAGIC   round(ol.quantityReserved, 2) quantityReserved,
# MAGIC   round(ol.quantityCancelled, 2) quantityCancelled,
# MAGIC   round(ol.quantityShipped, 2) quantityShipped,
# MAGIC   round(ol.quantityReserved * ol.pricePerUnit, 2) reservedAmount,
# MAGIC   
# MAGIC   round(
# MAGIC     ol.quantityReserved * ol.pricePerUnit / exchangeSpotRate,
# MAGIC     2
# MAGIC   ) reservedAmountUsd,
# MAGIC   round(ol.quantityOrdered * ol.caseUomConv, 2) quantityOrderedCase,
# MAGIC   round(ol.quantityOrdered * ol.ansStdUomConv, 2) quantityOrderedAnsStduom,
# MAGIC   round(ol.quantityShipped * ol.caseUomConv, 2) quantityShippedCase,
# MAGIC   round(ol.quantityShipped * ol.ansStdUomConv, 2) quantityShippedAnsStduom,
# MAGIC   round(ol.orderAmount, 2) orderAmount,
# MAGIC   round(ol.quantityBackOrdered * ol.caseUomConv, 2) quantityBackOrderedCase,
# MAGIC   round(ol.quantityBackOrdered * ol.ansStdUomConv, 2) quantityBackOrderedAnsStdUom,
# MAGIC   ol.pricePerUnit,
# MAGIC   round(ol.orderAmount / oh.exchangeSpotRate, 2) orderAmountUsd,
# MAGIC   ol.apacScheduledShipDate,
# MAGIC   ol.orderUomCode,
# MAGIC   round(ol.pricePerUnit * ol.quantityCancelled, 2) cancelledAmount,
# MAGIC   round(
# MAGIC     ol.pricePerUnit * ol.quantityCancelled / oh.exchangeSpotRate,
# MAGIC     2
# MAGIC   ) cancelledAmountUsd,
# MAGIC   oh.transactionCurrencyId,
# MAGIC   oh.orderHeaderStatus,
# MAGIC   oh.customer_ID,
# MAGIC   oh.orderType_ID,
# MAGIC   oh.owningBusinessUnit_ID operatingUnit_ID,
# MAGIC   ol.inventoryWarehouse_ID inventoryUnit_ID,
# MAGIC   ol.item_id product_ID,
# MAGIC   oh.createdBy_ID headerCreatedBy,
# MAGIC   ol.createdBy_ID lineCreatedBy,
# MAGIC   oh.modifiedBy_ID headerModifiedBy,
# MAGIC   ol.modifiedBy_ID lineModifiedBy,
# MAGIC   oh.party_ID,
# MAGIC   oh.customer_ID account_ID,
# MAGIC   case
# MAGIC     when ol.deliveryNoteId is null then 0
# MAGIC     else round(ol.quantityOrdered, 2)
# MAGIC   end quantityPickSlip,
# MAGIC   case
# MAGIC     when ol.deliveryNoteId is null then 0
# MAGIC     else round(ol.quantityOrdered * ol.caseUomConv, 2)
# MAGIC   end quantityPickSlipCase,
# MAGIC   case
# MAGIC     when ol.deliveryNoteId is null then 0
# MAGIC     else round(ol.quantityOrdered * ol.ansStdUomConv, 2)
# MAGIC   end quantityPickSlipAnsStdUom,
# MAGIC   case
# MAGIC     when ol.deliveryNoteId is null then 0
# MAGIC     else round(ol.orderAmount, 2)
# MAGIC   end pickSlipAmount,
# MAGIC   case
# MAGIC     when ol.deliveryNoteId is null then 0
# MAGIC     else round(ol.orderAmount / oh.exchangeSpotRate, 2)
# MAGIC   end pickSlipAmountUsd,
# MAGIC   case
# MAGIC     when ol.quantityReserved = 0
# MAGIC     and ol.deliveryNoteId is null
# MAGIC     and apacScheduledShipDate <> '' then round(ol.orderAmount, 2)
# MAGIC     else 0
# MAGIC   end notAllocatedScheduledAmount,
# MAGIC   case
# MAGIC     when ol.quantityReserved = 0
# MAGIC     and ol.deliveryNoteId is null
# MAGIC     and apacScheduledShipDate <> '' then round(ol.orderAmount / oh.exchangeSpotRate, 2)
# MAGIC     else 0
# MAGIC   end notAllocatedScheduledAmountUsd,
# MAGIC   case
# MAGIC     when ol.quantityReserved = 0
# MAGIC     and ol.deliveryNoteId is null
# MAGIC     and apacScheduledShipDate <> '' then round(ol.quantityOrdered, 2)
# MAGIC     else 0
# MAGIC   end quantityNotAllocatedScheduled,
# MAGIC   case
# MAGIC     when ol.quantityReserved = 0
# MAGIC     and ol.deliveryNoteId is null
# MAGIC     and apacScheduledShipDate <> '' then round(ol.quantityOrdered * ol.caseUomConv, 2)
# MAGIC     else 0
# MAGIC   end quantityNotAllocatedScheduledCase,
# MAGIC   case
# MAGIC     when ol.quantityReserved = 0
# MAGIC     and ol.deliveryNoteId is null
# MAGIC     and apacScheduledShipDate <> '' then round(ol.quantityOrdered * ol.ansStdUomConv, 2)
# MAGIC     else 0
# MAGIC   end quantityNotAllocatedScheduledAnsStdUom,
# MAGIC   case
# MAGIC     when ol.quantityReserved = 0
# MAGIC     and ol.deliveryNoteId is null
# MAGIC     and apacScheduledShipDate = '' then round(ol.orderAmount, 2)
# MAGIC     else 0
# MAGIC   end notAllocatedNotScheduledAmount,
# MAGIC   case
# MAGIC     when ol.quantityReserved = 0
# MAGIC     and ol.deliveryNoteId is null
# MAGIC     and apacScheduledShipDate = '' then round(ol.orderAmount / oh.exchangeSpotRate, 2)
# MAGIC     else 0
# MAGIC   end notAllocatedNotScheduledAmountUsd,
# MAGIC   case
# MAGIC     when ol.quantityReserved = 0
# MAGIC     and ol.deliveryNoteId is null
# MAGIC     and apacScheduledShipDate = '' then round(ol.quantityOrdered, 2)
# MAGIC     else 0
# MAGIC   end quantityNotAllocatedNotScheduled,
# MAGIC   case
# MAGIC     when ol.quantityReserved = 0
# MAGIC     and ol.deliveryNoteId is null
# MAGIC     and apacScheduledShipDate = '' then round(ol.quantityOrdered * ol.caseUomConv, 2)
# MAGIC     else 0
# MAGIC   end quantityNotAllocatedNotScheduledCase,
# MAGIC   case
# MAGIC     when ol.quantityReserved = 0
# MAGIC     and ol.deliveryNoteId is null
# MAGIC     and apacScheduledShipDate = '' then round(ol.quantityOrdered * ol.ansStdUomConv, 2)
# MAGIC     else 0
# MAGIC   end quantityNotAllocatedNotScheduledAnsStdUom,
# MAGIC   case
# MAGIC     when oh.orderHoldType in (
# MAGIC       'Ansell Credit Check Failure',
# MAGIC       'Credit Card Auth Failure',
# MAGIC       'Credit Check Failure'
# MAGIC     ) then 'Credit Hold'
# MAGIC   end creditHoldFlag,
# MAGIC   oh.orderHoldType,
# MAGIC   ol.orderLineHoldType,
# MAGIC   ol.supplierNumber,
# MAGIC   ol.supplierName,
# MAGIC   ol.shipToAddress_ID,
# MAGIC   oh.billToAddress_ID,
# MAGIC   ol.deliveryNoteId,
# MAGIC   ol.actualShipDate,
# MAGIC   oh.orderHoldDate1,
# MAGIC   oh.orderHoldDate2,
# MAGIC   ol.orderLineHoldDate1,
# MAGIC   ol.orderLineHoldDate2,
# MAGIC   ol.orderedItem,
# MAGIC   round(ol.quantityInvoiced,2) quantityInvoiced,
# MAGIC   round(ol.quantityInvoiced * ol.caseUomConv, 2) quantityInvoicedCase,
# MAGIC   round(ol.quantityInvoiced * ol.ansStdUomConv, 2) quantityInvoicedAnsStduom,
# MAGIC   round(ol.quantityInvoiced * pricePerUnit , 2) invoicedAmount,
# MAGIC   oh.baseCurrencyId,
# MAGIC   oh.paymentTerm_ID,
# MAGIC   ol.itemIdentifierType,
# MAGIC   ol._modified,
# MAGIC   ol.primaryUomCode,
# MAGIC   ol.dropShipPoNumber,
# MAGIC   ol.promiseDate,
# MAGIC   ol.promisedOnDate,
# MAGIC   ol.caseUomConv 
# MAGIC from
# MAGIC   s_supplychain.sales_order_headers_agg oh
# MAGIC   join s_supplychain.sales_order_lines_agg ol on ol.salesorder_id = oh._ID
# MAGIC   join s_core.TRANSACTION_TYPE_AGG ot on oh.orderType_ID = ot._ID
# MAGIC   join s_core.organization_agg ou on oh.owningBusinessUnit_ID = ou._ID
# MAGIC where
# MAGIC   oh._source = 'EBS'
# MAGIC   and ou.organizationCode = '5100'
# MAGIC   and not OL._DELETED
# MAGIC   and not OH._DELETED
# MAGIC   and not ot._DELETED

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW g_mex_sm.sm_customer_location_v AS
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
# MAGIC   customer_location_agg.stateName,
# MAGIC   customer_location_agg.territory_ID
# MAGIC from
# MAGIC   s_core.customer_location_agg
# MAGIC where
# MAGIC   customer_location_agg._source = 'EBS'
# MAGIC   and not customer_location_agg._deleted;  

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW g_mex_sm.sm_order_types_v AS
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
