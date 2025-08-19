# Databricks notebook source
# MAGIC %sql
# MAGIC create database if not exists  g_ecomm

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW g_ecomm.ecomm_products_v AS
# MAGIC select
# MAGIC     product_agg._ID,
# MAGIC     product_agg.ansStdUom,
# MAGIC     product_agg.ansStdUomConv,
# MAGIC     product_agg.brandFamily,
# MAGIC     product_agg.brandStrategy,
# MAGIC     product_agg.dimensionUomCode,
# MAGIC     product_agg.gbu,
# MAGIC     product_agg.itemId,
# MAGIC     product_agg.itemType,
# MAGIC     product_agg.legacyAspn,
# MAGIC     product_agg.lowestShippableUom,
# MAGIC     product_agg.marketingCode,
# MAGIC     product_agg.mrpn,
# MAGIC     product_agg.name,
# MAGIC     product_agg.originId as originCode,
# MAGIC     product_agg.originDescription,
# MAGIC     product_agg.piecesInAltCase,
# MAGIC     product_agg.piecesInBag,
# MAGIC     product_agg.piecesInBox,
# MAGIC     product_agg.piecesInBundle,
# MAGIC     product_agg.piecesInCarton,
# MAGIC     product_agg.piecesInCase,
# MAGIC     product_agg.piecesinDispenserDisplay,
# MAGIC     product_agg.piecesInDisplayDispenser,
# MAGIC     product_agg.piecesInDozen,
# MAGIC     product_agg.piecesInDrum,
# MAGIC     product_agg.piecesInEach,
# MAGIC     product_agg.piecesInGross,
# MAGIC     product_agg.piecesInKit,
# MAGIC     product_agg.piecesInPack,
# MAGIC     product_agg.piecesInRoll,
# MAGIC     product_agg.piecesInRtlPack,
# MAGIC     product_agg.primarySellingUom,
# MAGIC     product_agg.primaryTechnology,
# MAGIC     product_agg.productBrand,
# MAGIC     product_agg.productBrandType,
# MAGIC     product_agg.productCategory,
# MAGIC     product_agg.productCode,
# MAGIC     product_agg.productDivision,
# MAGIC     product_agg.productFamily,
# MAGIC     product_agg.productGroup,
# MAGIC     product_agg.productLegacy,
# MAGIC     product_agg.productM4Category,
# MAGIC     product_agg.productM4Family,
# MAGIC     product_agg.productM4Group,
# MAGIC     product_agg.productM4Segment,
# MAGIC     product_agg.productMaterialSubType,
# MAGIC     product_agg.productMaterialType,
# MAGIC     product_agg.productNps,
# MAGIC     product_agg.productSbu,
# MAGIC     product_agg.productStatus,
# MAGIC     product_agg.productStyle,
# MAGIC     product_agg.productStyleDescription,
# MAGIC     product_agg.productSubBrand,
# MAGIC     product_agg.productSubCategory,
# MAGIC     product_agg.productTransferPrice,
# MAGIC     product_agg.productType,
# MAGIC     product_agg.productVariant,
# MAGIC     product_agg.styleItem,
# MAGIC     product_agg.productStatusEffectiveDate,
# MAGIC     product_agg.primaryUomConv,
# MAGIC     product_agg.secondarySellingUom,
# MAGIC     product_agg.productStatusImplementedDate,
# MAGIC     product_agg.productStatusChangeDate,
# MAGIC     product_agg.launchDate,
# MAGIC     product_agg.caseLength,
# MAGIC     product_agg.caseWidth,
# MAGIC     product_agg.caseHeight,
# MAGIC     product_agg.shelfLife,
# MAGIC     product_agg.caseVolume,
# MAGIC     product_agg.volumeUomCode,
# MAGIC     product_agg.caseNetWeight,
# MAGIC     product_agg.weightUomCode,
# MAGIC     product_origin_agg.commoditycode,
# MAGIC     product_origin_agg.primaryorigin
# MAGIC from
# MAGIC   s_core.product_agg
# MAGIC   left join (select productcode,  originid, primaryorigin, max(product_origin_agg.commoditycode) commoditycode
# MAGIC              from s_core.product_origin_agg where _SOURCE = 'PDH' and not _deleted
# MAGIC              group by productcode,  originid, primaryorigin ) product_origin_agg on
# MAGIC     product_agg.productcode = product_origin_agg.productcode   and product_agg.originId = product_origin_agg.originid
# MAGIC where
# MAGIC   product_agg._SOURCE = 'EBS'
# MAGIC   AND not product_agg._DELETED

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW g_ecomm.ecomm_products_org_v as
# MAGIC select
# MAGIC   item_ID,
# MAGIC   organization_ID,
# MAGIC   productStatus
# MAGIC from s_core.product_org_agg
# MAGIC where _SOURCE = 'EBS'
# MAGIC   AND not _DELETED

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW g_ecomm.ecomm_operating_units_v AS 
# MAGIC SELECT
# MAGIC   _ID,
# MAGIC   name,
# MAGIC   organizationCode,
# MAGIC   organizationId
# MAGIC from s_core.organization_agg
# MAGIC WHERE _SOURCE = 'EBS'
# MAGIC   AND organizationType = 'OPERATING_UNIT'
# MAGIC   AND not _DELETED;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW g_ecomm.ecomm_inventory_units_v AS 
# MAGIC SELECT
# MAGIC   _ID,
# MAGIC   name,
# MAGIC   organizationCode,
# MAGIC   organizationId,
# MAGIC   case 
# MAGIC     when organizationCode = '325'
# MAGIC     then 'Drop Shipmnent'
# MAGIC     else 'Warehouse Shipment'
# MAGIC   end dropShipmentFlag
# MAGIC from s_core.organization_agg
# MAGIC WHERE _SOURCE = 'EBS'
# MAGIC   AND organizationType = 'INV'
# MAGIC   AND not _DELETED

# COMMAND ----------

# MAGIC %sql 
# MAGIC CREATE OR REPLACE VIEW g_ecomm.ecomm_accounts_v AS
# MAGIC select
# MAGIC   _ID,
# MAGIC   createdOn as accountCreatedDate,
# MAGIC   accountId,
# MAGIC   accountNumber,
# MAGIC   name,
# MAGIC   customerType,
# MAGIC   gbu,
# MAGIC   vertical,
# MAGIC   subVertical,
# MAGIC   industry,
# MAGIC   subIndustry,
# MAGIC   accountType,
# MAGIC   region,
# MAGIC   eCommerceFlag
# MAGIC from
# MAGIC   s_core.account_agg
# MAGIC where
# MAGIC   account_agg._source = 'EBS'
# MAGIC   and not account_agg._DELETED
# MAGIC   and eCommerceFlag = true

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW g_ecomm.ecomm_origin_v AS
# MAGIC select
# MAGIC   _ID,
# MAGIC   ansellPlant,
# MAGIC   originId,
# MAGIC   originName
# MAGIC from
# MAGIC   s_core.origin_agg
# MAGIC where
# MAGIC   origin_agg._source = 'EBS'
# MAGIC   and not _DELETED;  

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace view g_ecomm.ecomm_sales_orders_v as
# MAGIC select
# MAGIC   ol.salesOrderDetailId,
# MAGIC   oh.orderDate,
# MAGIC   year(oh.orderDate) orderedYear,
# MAGIC   year(oh.orderDate) || ' / ' || month(oh.orderDate) OrderedMonth,
# MAGIC   int(date_format(oh.orderDate, 'yyyyMMdd') ) orderDateId,
# MAGIC   ol.requestDeliveryBy requestDate,
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
# MAGIC   ol.lineNumber || '.' ||  ol.sequenceNumber line,
# MAGIC   ol.returnReasonCode ,
# MAGIC   round(ol.returnAmount, 2) returnAmount,
# MAGIC   round(ol.returnAmount / exchangeRateUsd,2) returnAmountUsd,
# MAGIC   round(ol.quantityReturned *  ol.primaryUomConv,2) quantityReturned,
# MAGIC   round(ol.backOrderValue, 2) backOrderValue,
# MAGIC   round(ol.backOrderValue / oh.exchangeRateUsd,2) backOrderValueUsd,
# MAGIC   round(ol.quantityBackOrdered , 2) quantityBackOrdered,
# MAGIC   round(ol.quantityOrdered, 2) quantityOrdered,
# MAGIC   round(ol.quantityReserved, 2) quantityReserved ,
# MAGIC   round(ol.quantityReserved  * ol.pricePerUnit, 2) reservedAmount,
# MAGIC   round(ol.quantityReserved  * ol.pricePerUnit / exchangeRateUsd,2) reservedAmountUsd,
# MAGIC   round(ol.quantityOrdered * ol.caseUomConv, 2) quantityOrderedCase,
# MAGIC   round(ol.quantityReserved * ol.caseUomConv, 2) quantityReservedCase,
# MAGIC   round(ol.quantityOrdered * ol.ansStdUomConv, 2) quantityOrderedAnsStduom,
# MAGIC   round(ol.quantityReserved * ol.ansStdUomConv, 2) quantityReservedAnsStduom,
# MAGIC   round(ol.quantityOrdered * ol.primaryUomConv, 2) quantityOrderedPrimary,
# MAGIC   round(ol.quantityReserved * ol.primaryUomConv, 2) quantityReservedPrimary,
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
# MAGIC   oh.orderType_ID,
# MAGIC   oh.owningBusinessUnit_ID operatingUnit_ID,
# MAGIC   ol.inventoryWarehouse_ID inventoryUnit_ID,
# MAGIC   ol.item_id product_ID,
# MAGIC   oh.createdBy_ID headerCreatedBy_ID,
# MAGIC   ol.createdBy_ID lineCreatedBy_ID,
# MAGIC   oh.modifiedBy_ID headerModifiedBy_ID,
# MAGIC   ol.modifiedBy_ID lineModifiedBy_ID,
# MAGIC   oh.customer_ID account_ID,
# MAGIC   oh.orderHoldType,
# MAGIC   ol.orderLineHoldType,
# MAGIC   ol.supplierNumber,
# MAGIC   ol.supplierName,
# MAGIC   ol.shipToAddress_ID,
# MAGIC   oh.billToAddress_ID,
# MAGIC   ol.deliveryNoteId,
# MAGIC   ol.actualShipDate,
# MAGIC   ol.orderedItem,
# MAGIC   ol.scheduledShipDate,
# MAGIC   ol.orderAmount /* -seeThruCost */ seeThruMargin,
# MAGIC   oh.orderHoldBy1_ID  orderHoldBy1_ID,
# MAGIC   oh.orderHoldBy2_ID  orderHoldBy2_ID,
# MAGIC   ol.orderLineHoldBy1_ID orderLineHoldBy1_ID,
# MAGIC   ol.orderLineHoldBy2_ID orderLineHoldBy2_ID,
# MAGIC   oh.orderHoldDate1 orderHoldDate1,
# MAGIC   oh.orderHoldDate2 orderHoldDate2,
# MAGIC   ol.orderLineHoldDate1 orderLineHoldDate1,
# MAGIC   ol.orderLineHoldDate2 orderLineHoldDate2,
# MAGIC   oh.distributionchannel ,
# MAGIC   oh.sourceOrderReference
# MAGIC from
# MAGIC   s_supplychain.sales_order_headers_agg oh
# MAGIC   join s_supplychain.sales_order_lines_agg ol on ol.salesorder_id = oh._ID
# MAGIC   join s_core.TRANSACTION_TYPE_AGG ot on oh.orderType_ID = ot._ID
# MAGIC   join s_core.account_agg account on oh.customer_ID = account._ID
# MAGIC where
# MAGIC   oh._source = 'EBS'
# MAGIC   and ol.bookedFlag = 'Y'
# MAGIC   and not OL._DELETED
# MAGIC   and not OH._DELETED
# MAGIC   and not ot._DELETED
# MAGIC   and account.ecommerceFlag = true

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW g_ecomm.ecomm_customer_location_v AS
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
# MAGIC   and not customer_location_agg._DELETED 

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW g_ecomm.ecomm_order_types_v AS
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
# MAGIC   and not transaction_type_agg._DELETED  

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW g_ecomm.ecomm_transaction_types_v AS
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
# MAGIC   and not transaction_type_agg._DELETED; 

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW g_ecomm.ecomm_users_v AS
# MAGIC select 
# MAGIC   _ID,
# MAGIC   login,
# MAGIC   firstName,
# MAGIC   lastName,
# MAGIC   fullName
# MAGIC from 
# MAGIC   s_core.user_agg
# MAGIC where 
# MAGIC   _source = 'EBS'
# MAGIC   and not _DELETED

# COMMAND ----------

# MAGIC %sql 
# MAGIC CREATE OR REPLACE VIEW g_ecomm.ecomm_territory_v AS
# MAGIC select
# MAGIC   _ID,
# MAGIC   territoryCode,
# MAGIC   territoryShortName,
# MAGIC   territoryDescription,
# MAGIC   euCode,
# MAGIC   --subRegion,
# MAGIC   --subRegionGis,
# MAGIC   --managementArea,
# MAGIC   --region,
# MAGIC   --forecastArea,
# MAGIC   obsoleteFlag,
# MAGIC   isoCodeNumeric,
# MAGIC   isoCode,
# MAGIC   nlsCode
# MAGIC from
# MAGIC   s_core.territory_agg
# MAGIC where
# MAGIC   _source = 'EBS'
# MAGIC   and not _deleted
