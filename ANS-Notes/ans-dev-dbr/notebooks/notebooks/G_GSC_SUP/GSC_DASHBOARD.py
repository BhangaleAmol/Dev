# Databricks notebook source
# Purpose: input for  gold supply chain CBO dashboard
# Author: Guy De Paepe
# Creation Date: Mar 2021
# Modifications:
#

# COMMAND ----------

# MAGIC %run ../_SHARED/FUNCTIONS/1.2/bootstrap

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS g_gsc_sup

# COMMAND ----------

# MAGIC %sql 
# MAGIC CREATE OR REPLACE VIEW g_gsc_sup.gsc_product_v AS
# MAGIC select 
# MAGIC   _ID,
# MAGIC   ansStdUom,
# MAGIC   itemId, 
# MAGIC   productCode, 
# MAGIC   case
# MAGIC     when itemType not in ('FINISHED GOODS')
# MAGIC       then 'N/A'
# MAGIC     when gbu in ('MEDICAL')
# MAGIC          or productSbu in ('HSS', 'EXAM', 'SU/EXAM', 'SURG' )
# MAGIC          or productDivision in ('HSS', 'SURG')
# MAGIC          or productDivision like 'MED%'
# MAGIC          or productDivision like 'SU%'
# MAGIC          or productDivision like 'LS%'
# MAGIC        then 'MEDICAL'
# MAGIC     when gbu in ('INDUSTRIAL')
# MAGIC         or productDivision in ('INDUSTRIAL', 'NV&AC')
# MAGIC         or productDivision like 'BP%'
# MAGIC         or productDivision like 'MECH%'
# MAGIC         or productDivision like 'CHEM%'
# MAGIC         or productSbu like '%IND%'
# MAGIC       then 'INDUSTRIAL'
# MAGIC     else
# MAGIC       'N/A'
# MAGIC   end gbu, 
# MAGIC   _source, 
# MAGIC   productsbu, 
# MAGIC   productdivision,
# MAGIC   productStyle,
# MAGIC   productM4Family,
# MAGIC   'A' productWarehouseAbcCode,
# MAGIC   sizeDescription
# MAGIC from s_core.product_agg
# MAGIC where _source not in ('TWD', 'SF', 'TOT')
# MAGIC   and productDivision not in ('SH&WB')
# MAGIC   and not product_agg._DELETED
# MAGIC order by productcode, _source

# COMMAND ----------

# MAGIC %sql 
# MAGIC CREATE OR REPLACE VIEW g_gsc_sup.gsc_organization_v AS
# MAGIC select 
# MAGIC   _ID, 
# MAGIC   organizationId, 
# MAGIC   organizationCode, 
# MAGIC   organizationCode || '-' || name name
# MAGIC from s_core.organization_agg
# MAGIC where organization_agg._source not in ('TWD', 'SF', 'TOT')
# MAGIC  and not organization_agg._DELETED

# COMMAND ----------

# MAGIC %sql 
# MAGIC CREATE OR REPLACE VIEW g_gsc_sup.gsc_account_v AS
# MAGIC select 
# MAGIC   _Id,
# MAGIC   accountId,
# MAGIC   accountNumber, 
# MAGIC   name
# MAGIC from s_core.account_agg
# MAGIC where account_agg._source not in ('TWD', 'SF', 'TOT')
# MAGIC   and not account_agg._DELETED

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW g_gsc_sup.wc_qv_order_data_v AS
# MAGIC select
# MAGIC   ol.salesorderDetailId KEY,
# MAGIC   org.organizationCode Company_Number_Detail,
# MAGIC   pr.productCode PROD_NUM,
# MAGIC   oh.orderNumber ORDER_NUM,
# MAGIC   ol.lineNumber ORDER_LINE_NUM,
# MAGIC   ol.sequenceNumber ORDER_LINE_DETAIL_NUM,
# MAGIC   date_format(oh.orderDate, 'MM/dd/yyyy') ORDER_DT,
# MAGIC   date_format(ol.requestDeliveryBy, 'MM/dd/yyyy') REQUEST_DT,
# MAGIC   date_format(ol.scheduledShipDate, 'MM/dd/yyyy') SCHEDULE_DT,
# MAGIC   date_format(ol.actualShipDate, 'MM/dd/yyyy') SHIP_DT,
# MAGIC   case when upper(ol.orderLineStatus) in ('CLOSED', 'CANCELLED')  
# MAGIC     then ol.orderLineStatus
# MAGIC     when (oh.orderHoldType is not null or ol.orderLineHoldType is not null)
# MAGIC     then 'Blocked'
# MAGIC    else ol.orderLineStatus
# MAGIC   end ORDER_STATUS,
# MAGIC   ol.quantityOrdered * ol.ansStdUomConv ORDER_QTY_STD_UOM,
# MAGIC   ol.orderAmount ORDERED_AMOUNT_DOC_CURR,
# MAGIC   oh.baseCurrencyId LE_CURRENCY,
# MAGIC   ac.accountNumber CUSTUMER_ID,
# MAGIC   st.partySiteNumber SHIP_TO_DELIVERY_LOCATION_ID,
# MAGIC   tt.description ORDER_TYPE,
# MAGIC   oh.customerPONumber CUST_PO_NUM,
# MAGIC    case when upper(ol.orderStatusDetail) in ('CLOSED', 'CANCELLED')  
# MAGIC      then ol.orderStatusDetail
# MAGIC      when (oh.orderHoldType is not null or ol.orderLineHoldType is not null)
# MAGIC      then  'On Hold'
# MAGIC     else ol.orderStatusDetail
# MAGIC    end SOURCE_ORDER_STATUS,
# MAGIC   inv.organizationCode ORGANIZATION_CODE,
# MAGIC   date_format(greatest(ol.modifiedOn, oh.modifiedOn), 'MM/dd/yyyy') W_UPDATE_DT,
# MAGIC   oh.transactionCurrencyId DOC_CURR_CODE,
# MAGIC   ac.accountNumber DISTRIBUTOR_ID,
# MAGIC   ac.name DISTRIBUTOR_NAME,
# MAGIC   ol.deliveryNoteId X_DELIVERY_NOTE_ID,
# MAGIC   case when upper(ol.orderStatusDetail) not in ('CLOSED', 'CANCELLED')
# MAGIC     then coalesce(oh.orderHoldType, ol.orderLineHoldType)
# MAGIC   else ''
# MAGIC   end X_ORDER_HOLD_TYPE,
# MAGIC   ol.quantityReserved X_RESERVATION_VALUE,
# MAGIC   ol.intransitTime X_INTRANSIT_TIME,
# MAGIC   date_format(ol.shipDate945, 'MM/dd/yyyy') X_SHIPDATE_945,
# MAGIC   oh.freightTerms X_FREIGHT_TERMS,
# MAGIC   date_format(ol.createdOn, 'MM/dd/yyyy') X_CREATION_DATE,
# MAGIC   date_format(ol.bookedDate, 'MM/dd/yyyy') X_BOOKED_DATE,
# MAGIC   ol.quantityShipped X_SHIPPED_QUANTITY,
# MAGIC   ol.shippingQuantityUom X_SHIPPING_QUANTITY_UOM,
# MAGIC   date_format(ol.needByDate, 'MM/dd/yyyy') X_NEED_BY_DATE,
# MAGIC   date_format(ol.retd, 'MM/dd/yyyy') X_RETD,
# MAGIC   date_format(ol.promiseDate, 'MM/dd/yyyy')  X_PROMISED_DATE,
# MAGIC   date_format(ol.CETD, 'MM/dd/yyyy')        X_CETD,
# MAGIC   date_format(oh.orderDate, 'MM/dd/yyyy') ORDER_DATE_TIME,
# MAGIC   date_format(ol.promisedOnDate, 'MM/dd/yyyy') SO_PROMISED_DATE,
# MAGIC   CURRENT_DATE CREATEDDATE,
# MAGIC   0 STATUS,
# MAGIC   pm.paymentMethodName,
# MAGIC   pt.paymentTermName  
# MAGIC from
# MAGIC   s_supplychain.sales_order_headers_agg oh
# MAGIC   join s_supplychain.sales_order_lines_agg ol on oh._ID = ol.salesOrder_ID
# MAGIC   join s_core.organization_agg org on oh.owningBusinessUnit_ID = org._ID
# MAGIC   join s_core.organization_agg inv on ol.inventoryWarehouse_ID = inv._ID
# MAGIC   join s_core.product_agg pr on ol.item_ID = pr._ID
# MAGIC   join s_core.account_agg ac on oh.customer_ID = ac._ID
# MAGIC   join s_core.customer_location_agg st on ol.shipToAddress_ID = st._ID
# MAGIC   join s_core.transaction_type_agg tt on oh.orderType_ID = tt._ID
# MAGIC   left join s_core.payment_methods_agg pm on oh.paymentMethod_ID = pm._id
# MAGIC   join s_core.payment_terms_agg pt on oh.paymentTerm_ID = pt._id
# MAGIC where
# MAGIC   oh._source = 'EBS'
# MAGIC   and upper(tt.description) not like '%SAMPLE%'
# MAGIC   and upper(tt.description) not like '%SAFETY STOCK%'
# MAGIC   and upper(tt.description) not like '%RETURN%'
# MAGIC   and upper(tt.description) not like '%CREDIT MEMO%'
# MAGIC   and upper(tt.description) not like '%REPLACEMENT%'
# MAGIC   and upper(tt.description) not like '%SERVICE%'
# MAGIC   and upper(tt.description) not like '%INTERCOMPANY%'
# MAGIC   and upper(tt.description) not like '%RMA%'
# MAGIC   and ac.customertype = 'External'
# MAGIC   and ol.bookedflag = 'Y'
# MAGIC   and (ol.modifiedOn > current_date - 7
# MAGIC    or oh.modifiedOn > current_date - 7 )
# MAGIC
# MAGIC   
# MAGIC   
# MAGIC   
# MAGIC   
# MAGIC   --and year(oh.orderDate) = 2021
# MAGIC   --and month(oh.orderDate) = 4
# MAGIC   --and salesorderDetailId = 12748800
# MAGIC  -- and ol.modifiedOn > current_date - 7
# MAGIC -- and oh.ordernumber = '100012058'

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace view  G_GSC_SUP.ALL_DC_AGING_V AS
# MAGIC SELECT distinct
# MAGIC   xsdl.inv_org_code,
# MAGIC   xsdl.sold_to_cust_name customer_name,
# MAGIC   xsdl.order_type,
# MAGIC   XSDL.SO_NUMBER,
# MAGIC   XSDL.delivery_id,
# MAGIC   date_format(XSDL.CREATION_DATE, 'yyyyMMdd') CREATION_DATE
# MAGIC FROM
# MAGIC   ebs.XX_SALES_ORDER_DETAILS_INT XSDL,
# MAGIC   ebs.wsh_new_deliveries wnd
# MAGIC where 1=1
# MAGIC   and wnd.delivery_id=XSDL.DELIVERY_ID
# MAGIC   and xsdl.ORGANIZATION_ID in (4324,126,127,3889)
# MAGIC   and wnd.status_code not in ('CL')
# MAGIC   and not exists (select 1 from ebs.XX_SO_DELIVERY_LOAD_WMSDTL_FL XSDLW
# MAGIC       where XSDLW.DELIVERYID = XSDL.DELIVERY_ID )
# MAGIC   and date_format(XSDL.CREATION_DATE, 'yyyyMMdd') between date_format(current_date - 365, 'yyyyMMdd') and date_format(current_date, 'yyyyMMdd')
# MAGIC   and sold_to_cust_name !='1'
# MAGIC order by XSDL.delivery_id

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW g_gsc_sup.wc_qv_order_data_full AS
# MAGIC select
# MAGIC   ol.salesorderDetailId KEY,
# MAGIC   org.organizationCode Company_Number_Detail,
# MAGIC   pr.productCode PROD_NUM,
# MAGIC   oh.orderNumber ORDER_NUM,
# MAGIC   ol.lineNumber ORDER_LINE_NUM,
# MAGIC   ol.sequenceNumber ORDER_LINE_DETAIL_NUM,
# MAGIC   date_format(oh.orderDate, 'MM/dd/yyyy') ORDER_DT,
# MAGIC   date_format(ol.requestDeliveryBy, 'MM/dd/yyyy') REQUEST_DT,
# MAGIC   date_format(ol.scheduledShipDate, 'MM/dd/yyyy') SCHEDULE_DT,
# MAGIC   date_format(ol.actualShipDate, 'MM/dd/yyyy') SHIP_DT,
# MAGIC   case when upper(ol.orderLineStatus) in ('CLOSED', 'CANCELLED')  
# MAGIC     then ol.orderLineStatus
# MAGIC     when (oh.orderHoldType is not null or ol.orderLineHoldType is not null)
# MAGIC     then 'Blocked'
# MAGIC    else ol.orderLineStatus
# MAGIC   end ORDER_STATUS,
# MAGIC   ol.quantityOrdered * ol.ansStdUomConv ORDER_QTY_STD_UOM,
# MAGIC   ol.orderAmount ORDERED_AMOUNT_DOC_CURR,
# MAGIC   oh.baseCurrencyId LE_CURRENCY,
# MAGIC   ac.accountNumber CUSTUMER_ID,
# MAGIC   st.partySiteNumber SHIP_TO_DELIVERY_LOCATION_ID,
# MAGIC   tt.description ORDER_TYPE,
# MAGIC   oh.customerPONumber CUST_PO_NUM,
# MAGIC    case when upper(ol.orderStatusDetail) in ('CLOSED', 'CANCELLED')  
# MAGIC      then ol.orderStatusDetail
# MAGIC      when (oh.orderHoldType is not null or ol.orderLineHoldType is not null)
# MAGIC      then  'On Hold'
# MAGIC     else ol.orderStatusDetail
# MAGIC    end SOURCE_ORDER_STATUS,
# MAGIC   inv.organizationCode ORGANIZATION_CODE,
# MAGIC   date_format(greatest(ol.modifiedOn, oh.modifiedOn), 'MM/dd/yyyy') W_UPDATE_DT,
# MAGIC   oh.transactionCurrencyId DOC_CURR_CODE,
# MAGIC   ac.accountNumber DISTRIBUTOR_ID,
# MAGIC   ac.name DISTRIBUTOR_NAME,
# MAGIC   ol.deliveryNoteId X_DELIVERY_NOTE_ID,
# MAGIC   case when upper(ol.orderStatusDetail) not in ('CLOSED', 'CANCELLED')
# MAGIC     then coalesce(oh.orderHoldType, ol.orderLineHoldType)
# MAGIC   else ''
# MAGIC   end X_ORDER_HOLD_TYPE,
# MAGIC   ol.quantityReserved X_RESERVATION_VALUE,
# MAGIC   ol.intransitTime X_INTRANSIT_TIME,
# MAGIC   date_format(ol.shipDate945, 'MM/dd/yyyy') X_SHIPDATE_945,
# MAGIC   oh.freightTerms X_FREIGHT_TERMS,
# MAGIC   date_format(ol.createdOn, 'MM/dd/yyyy') X_CREATION_DATE,
# MAGIC   date_format(ol.bookedDate, 'MM/dd/yyyy') X_BOOKED_DATE,
# MAGIC   ol.quantityShipped X_SHIPPED_QUANTITY,
# MAGIC   ol.shippingQuantityUom X_SHIPPING_QUANTITY_UOM,
# MAGIC   date_format(ol.needByDate, 'MM/dd/yyyy') X_NEED_BY_DATE,
# MAGIC   date_format(ol.retd, 'MM/dd/yyyy') X_RETD,
# MAGIC   date_format(ol.promiseDate, 'MM/dd/yyyy')  X_PROMISED_DATE,
# MAGIC   date_format(ol.CETD, 'MM/dd/yyyy')        X_CETD,
# MAGIC   date_format(oh.orderDate, 'MM/dd/yyyy') ORDER_DATE_TIME,
# MAGIC   date_format(ol.promisedOnDate, 'MM/dd/yyyy') SO_PROMISED_DATE,
# MAGIC   CURRENT_DATE CREATEDDATE,
# MAGIC   0 STATUS,
# MAGIC   pm.paymentMethodName,
# MAGIC   pt.paymentTermName  
# MAGIC from
# MAGIC   s_supplychain.sales_order_headers_agg oh
# MAGIC   join s_supplychain.sales_order_lines_agg ol on oh._ID = ol.salesOrder_ID
# MAGIC   join s_core.organization_agg org on oh.owningBusinessUnit_ID = org._ID
# MAGIC   join s_core.organization_agg inv on ol.inventoryWarehouse_ID = inv._ID
# MAGIC   join s_core.product_agg pr on ol.item_ID = pr._ID
# MAGIC   join s_core.account_agg ac on oh.customer_ID = ac._ID
# MAGIC   join s_core.customer_location_agg st on ol.shipToAddress_ID = st._ID
# MAGIC   join s_core.transaction_type_agg tt on oh.orderType_ID = tt._ID
# MAGIC   left join s_core.payment_methods_agg pm on oh.paymentMethod_ID = pm._id
# MAGIC   join s_core.payment_terms_agg pt on oh.paymentTerm_ID = pt._id
# MAGIC where
# MAGIC   oh._source = 'EBS'
# MAGIC   and upper(tt.description) not like '%SAMPLE%'
# MAGIC   and upper(tt.description) not like '%SAFETY STOCK%'
# MAGIC   and upper(tt.description) not like '%RETURN%'
# MAGIC   and upper(tt.description) not like '%CREDIT MEMO%'
# MAGIC   and upper(tt.description) not like '%REPLACEMENT%'
# MAGIC   and upper(tt.description) not like '%SERVICE%'
# MAGIC   and upper(tt.description) not like '%INTERCOMPANY%'
# MAGIC   and upper(tt.description) not like '%RMA%'
# MAGIC   and ac.customertype = 'External'
# MAGIC   and ol.bookedflag = 'Y'

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW g_gsc_sup.wc_qv_open_order_data_v AS
# MAGIC select
# MAGIC   ol.salesorderDetailId KEY,
# MAGIC   org.organizationCode Company_Number_Detail,
# MAGIC   pr.productCode PROD_NUM,
# MAGIC   oh.orderNumber ORDER_NUM,
# MAGIC   ol.lineNumber ORDER_LINE_NUM,
# MAGIC   ol.sequenceNumber ORDER_LINE_DETAIL_NUM,
# MAGIC   date_format(oh.orderDate, 'MM/dd/yyyy') ORDER_DT,
# MAGIC   date_format(ol.requestDeliveryBy, 'MM/dd/yyyy') REQUEST_DT,
# MAGIC   date_format(ol.scheduledShipDate, 'MM/dd/yyyy') SCHEDULE_DT,
# MAGIC   date_format(ol.actualShipDate, 'MM/dd/yyyy') SHIP_DT,
# MAGIC   case when upper(ol.orderLineStatus) in ('CLOSED', 'CANCELLED')  
# MAGIC     then ol.orderLineStatus
# MAGIC     when (oh.orderHoldType is not null or ol.orderLineHoldType is not null)
# MAGIC     then 'Blocked'
# MAGIC    else ol.orderLineStatus
# MAGIC   end ORDER_STATUS,
# MAGIC   ol.quantityOrdered * ol.ansStdUomConv ORDER_QTY_STD_UOM,
# MAGIC   ol.orderAmount ORDERED_AMOUNT_DOC_CURR,
# MAGIC   oh.baseCurrencyId LE_CURRENCY,
# MAGIC   ac.accountNumber CUSTUMER_ID,
# MAGIC   st.partySiteNumber SHIP_TO_DELIVERY_LOCATION_ID,
# MAGIC   tt.description ORDER_TYPE,
# MAGIC   oh.customerPONumber CUST_PO_NUM,
# MAGIC    case when upper(ol.orderStatusDetail) in ('CLOSED', 'CANCELLED')  
# MAGIC      then ol.orderStatusDetail
# MAGIC      when (oh.orderHoldType is not null or ol.orderLineHoldType is not null)
# MAGIC      then  'On Hold'
# MAGIC     else ol.orderStatusDetail
# MAGIC    end SOURCE_ORDER_STATUS,
# MAGIC   inv.organizationCode ORGANIZATION_CODE,
# MAGIC   date_format(greatest(ol.modifiedOn, oh.modifiedOn), 'MM/dd/yyyy') W_UPDATE_DT,
# MAGIC   oh.transactionCurrencyId DOC_CURR_CODE,
# MAGIC   ac.accountNumber DISTRIBUTOR_ID,
# MAGIC   ac.name DISTRIBUTOR_NAME,
# MAGIC   ol.deliveryNoteId X_DELIVERY_NOTE_ID,
# MAGIC   case when upper(ol.orderStatusDetail) not in ('CLOSED', 'CANCELLED')
# MAGIC     then coalesce(oh.orderHoldType, ol.orderLineHoldType)
# MAGIC   else ''
# MAGIC   end X_ORDER_HOLD_TYPE,
# MAGIC   ol.quantityReserved X_RESERVATION_VALUE,
# MAGIC   ol.intransitTime X_INTRANSIT_TIME,
# MAGIC   date_format(ol.shipDate945, 'MM/dd/yyyy') X_SHIPDATE_945,
# MAGIC   oh.freightTerms X_FREIGHT_TERMS,
# MAGIC   date_format(ol.createdOn, 'MM/dd/yyyy') X_CREATION_DATE,
# MAGIC   date_format(ol.bookedDate, 'MM/dd/yyyy') X_BOOKED_DATE,
# MAGIC   ol.quantityShipped X_SHIPPED_QUANTITY,
# MAGIC   ol.shippingQuantityUom X_SHIPPING_QUANTITY_UOM,
# MAGIC   date_format(ol.needByDate, 'MM/dd/yyyy') X_NEED_BY_DATE,
# MAGIC   date_format(ol.retd, 'MM/dd/yyyy') X_RETD,
# MAGIC   date_format(ol.promiseDate, 'MM/dd/yyyy')  X_PROMISED_DATE,
# MAGIC   date_format(ol.CETD, 'MM/dd/yyyy')        X_CETD,
# MAGIC   date_format(oh.orderDate, 'MM/dd/yyyy') ORDER_DATE_TIME,
# MAGIC   date_format(ol.promisedOnDate, 'MM/dd/yyyy') SO_PROMISED_DATE,
# MAGIC   CURRENT_DATE CREATEDDATE,
# MAGIC   0 STATUS,
# MAGIC   pm.paymentMethodName,
# MAGIC   pt.paymentTermName  
# MAGIC from
# MAGIC   s_supplychain.sales_order_headers_agg oh
# MAGIC   join s_supplychain.sales_order_lines_agg ol on oh._ID = ol.salesOrder_ID
# MAGIC   join s_core.organization_agg org on oh.owningBusinessUnit_ID = org._ID
# MAGIC   join s_core.organization_agg inv on ol.inventoryWarehouse_ID = inv._ID
# MAGIC   join s_core.product_agg pr on ol.item_ID = pr._ID
# MAGIC   join s_core.account_agg ac on oh.customer_ID = ac._ID
# MAGIC   join s_core.customer_location_agg st on ol.shipToAddress_ID = st._ID
# MAGIC   join s_core.transaction_type_agg tt on oh.orderType_ID = tt._ID
# MAGIC   left join s_core.payment_methods_agg pm on oh.paymentMethod_ID = pm._id
# MAGIC   join s_core.payment_terms_agg pt on oh.paymentTerm_ID = pt._id
# MAGIC where
# MAGIC   oh._source = 'EBS'
# MAGIC   and upper(tt.description) not like '%SAMPLE%'
# MAGIC   and upper(tt.description) not like '%SAFETY STOCK%'
# MAGIC   and upper(tt.description) not like '%RETURN%'
# MAGIC   and upper(tt.description) not like '%CREDIT MEMO%'
# MAGIC   and upper(tt.description) not like '%REPLACEMENT%'
# MAGIC   and upper(tt.description) not like '%SERVICE%'
# MAGIC   and upper(tt.description) not like '%INTERCOMPANY%'
# MAGIC   and upper(tt.description) not like '%RMA%'
# MAGIC   and ac.customertype = 'External'
# MAGIC   and ol.bookedflag = 'Y'
# MAGIC   and upper(ol.orderLineStatus) not in ('CLOSED', 'CANCELLED')

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW g_gsc_sup.wc_qv_inventory_valuation_a AS
# MAGIC Select 
# MAGIC     Product.productCode AS ITEM_NUMBER
# MAGIC     , Product.itemId AS INVENTORY_ITEM_ID
# MAGIC     , Organization.organizationCode AS ORGANIZATION_CODE
# MAGIC     , Inventory.primaryQty AS PRIMARY_QUANTITY
# MAGIC     , Inventory.primaryUOMCode AS PRIMARY_UOM_CODE
# MAGIC     , Inventory.secondaryQty AS SECONDARY_QUANTITY
# MAGIC     , Inventory.secondaryUOMCode AS SECONDARY_UOM_CODE
# MAGIC     , Product.name AS ITEM_DESCRIPTION
# MAGIC     , current_date - 1 AS TRANSDATE
# MAGIC     , Organization.name AS ORGANIZATION_NAME
# MAGIC     , Inventory.subInventoryCode AS SUBINVENTORY_CODE
# MAGIC     , Inventory.inventoryWarehouseID AS ORGANIZATION_ID
# MAGIC     , Inventory.dualUOMControl AS DUAL_UOM_CONTROL
# MAGIC     , Product.productDivision AS DIVISION
# MAGIC     , Product.itemType AS ITEM_TYPE
# MAGIC     , Inventory.glPeriodCode AS PERIOD_CODE
# MAGIC     , Inventory.lotNumber AS LOT_NUMBER
# MAGIC     , (Inventory.totalCost) * nvl(exch_rate.EXCH_RATE,1) AS STND_CST
# MAGIC     , ((Inventory.totalCost) * Inventory.primaryQty)* nvl(exch_rate.EXCH_RATE,1) AS EXTENDED_CST
# MAGIC     , (Inventory.sgaCostPerUnitPrimary) * nvl(exch_rate.EXCH_RATE,1) AS SGA_CST
# MAGIC     , (Inventory.sgaCostPerUnitPrimary * Inventory.primaryQty) * nvl(exch_rate.EXCH_RATE,1) AS EX_SGA_CST
# MAGIC     , Inventory.lotExpirationDate AS EXP_DATE
# MAGIC     , Inventory.lotControlFlag AS LOT_CONTROL_FLAG
# MAGIC     , Inventory.originId AS ORIGIN_CODE
# MAGIC     , Inventory.dateReceived AS DATE_RECEIVED
# MAGIC     , origin.originName AS ORIGIN_NAME
# MAGIC     , '' AS PO_NUMBER
# MAGIC     , Inventory.shelfLifeDays AS SHELF_LIFE_DAYS
# MAGIC     , Inventory.ansStdUomCode AS ANS_STD_UOM
# MAGIC     , Inventory.ansStdQty AS ANS_STD_QTY
# MAGIC     , case 
# MAGIC           when Inventory.lotExpirationDate is not null 
# MAGIC               and Inventory.lotExpirationDate <= add_months(current_date, 6) 
# MAGIC           then 'Hold' 
# MAGIC        end AS STATUS
# MAGIC     , Inventory.lotExpirationDate - Inventory.shelfLifeDays AS MFG_DATE
# MAGIC     , Product.marketingCode AS MRPN
# MAGIC     , Inventory._Source
# MAGIC from
# MAGIC   s_supplychain.inventory_agg Inventory
# MAGIC join s_core.product_agg Product
# MAGIC   on Inventory.item_id = Product._id
# MAGIC left join s_core.organization_agg Organization 
# MAGIC   on Inventory.inventoryWarehouse_ID = Organization._ID
# MAGIC left join s_core.origin_agg origin  
# MAGIC   on Inventory.origin_ID = origin._ID
# MAGIC left join (select aa.FROM_CURRENCY,aa.TO_CURRENCY,aa.CONVERSION_TYPE,aa.FROM_CURCY_CD,aa.TO_CURCY_CD,aa.RATE_TYPE,bb.CONVERSION_RATE EXCH_RATE, bb.CONVERSION_DATE CONVERSION_DATE from  EBS.GL_DAILY_RATES bb
# MAGIC join (select * from (
# MAGIC SELECT 
# MAGIC     GL_DAILY_RATES.FROM_CURRENCY, 
# MAGIC     GL_DAILY_RATES.TO_CURRENCY, 
# MAGIC     GL_DAILY_RATES.CONVERSION_TYPE, 
# MAGIC     GL_DAILY_RATES.FROM_CURRENCY FROM_CURCY_CD,
# MAGIC     GL_DAILY_RATES.TO_CURRENCY TO_CURCY_CD,
# MAGIC     GL_DAILY_RATES.CONVERSION_TYPE RATE_TYPE,
# MAGIC --     GL_DAILY_RATES.CONVERSION_RATE EXCH_RATE,
# MAGIC     MAX(GL_DAILY_RATES.CONVERSION_DATE) CONVERSION_DATE
# MAGIC   FROM 
# MAGIC     EBS.GL_DAILY_RATES
# MAGIC     where upper(CONVERSION_TYPE) = 'CORPORATE'
# MAGIC     and TO_CURRENCY = 'USD'     
# MAGIC     
# MAGIC     --and from_currency = 'CAD'
# MAGIC     GROUP BY
# MAGIC           GL_DAILY_RATES.FROM_CURRENCY, 
# MAGIC     GL_DAILY_RATES.TO_CURRENCY, 
# MAGIC     GL_DAILY_RATES.CONVERSION_TYPE, 
# MAGIC     GL_DAILY_RATES.FROM_CURRENCY ,
# MAGIC     GL_DAILY_RATES.TO_CURRENCY ,
# MAGIC     GL_DAILY_RATES.CONVERSION_TYPE )
# MAGIC --     GL_DAILY_RATES.CONVERSION_RATE)
# MAGIC     where    current_date-1 <=  CONVERSION_DATE) aa
# MAGIC on  aa.FROM_CURCY_CD = bb.FROM_CURRENCY and aa.TO_CURCY_CD = bb.TO_CURRENCY and aa.rate_type = bb.CONVERSION_TYPE and aa.CONVERSION_DATE = bb.CONVERSION_DATE) exch_rate
# MAGIC
# MAGIC on Organization.currency = exch_rate.FROM_CURCY_CD
# MAGIC Where Inventory._Source = 'EBS'
# MAGIC   and not inventory._DELETED
# MAGIC   and Organization.organizationCode not in ('532','600','601','602','605')
# MAGIC   and inventory.subinventorycode <>'IN TRANSIT'
# MAGIC   and inventory.inventoryDate = (select max(inventoryDate) from  s_supplychain.inventory_agg Inventory where not _deleted)
