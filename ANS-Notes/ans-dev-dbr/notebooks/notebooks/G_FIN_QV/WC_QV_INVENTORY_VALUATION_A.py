# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW g_fin_qv.wc_qv_inventory_valuation_a AS
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
# MAGIC     , Product.glProductDivision AS DIVISION
# MAGIC     , Product.itemType AS ITEM_TYPE
# MAGIC     , Inventory.glPeriodCode AS PERIOD_CODE
# MAGIC     , Inventory.lotNumber AS LOT_NUMBER
# MAGIC     , Inventory.totalCost AS STND_CST
# MAGIC     , (Inventory.totalCost) * Inventory.primaryQty AS EXTENDED_CST
# MAGIC     , Inventory.sgaCostPerUnitPrimary AS SGA_CST
# MAGIC     , Inventory.sgaCostPerUnitPrimary * Inventory.primaryQty AS EX_SGA_CST
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
# MAGIC Where Inventory._Source = 'EBS'
# MAGIC   and not inventory._DELETED
# MAGIC   and Organization.organizationCode not in ('532','600','601','602','605')
# MAGIC   and inventory.subinventorycode <>'IN TRANSIT'
