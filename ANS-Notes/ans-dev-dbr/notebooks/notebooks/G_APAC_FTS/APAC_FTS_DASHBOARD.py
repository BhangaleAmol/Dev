# Databricks notebook source
# MAGIC %sql
# MAGIC create database if not exists g_apac_fts

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW g_apac_fts.fts_products_v AS
# MAGIC select
# MAGIC product_agg._ID,
# MAGIC product_agg.ansStdUom,
# MAGIC product_agg.ansStdUomConv,
# MAGIC product_agg.brandFamily,
# MAGIC product_agg.brandStrategy,
# MAGIC product_agg.dimensionUomCode,
# MAGIC product_agg.gbu,
# MAGIC product_agg.itemChannel,
# MAGIC product_agg.itemId,
# MAGIC product_agg.itemPricingCategory,
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
# MAGIC product_agg.piecesinDispenserDisplay,
# MAGIC product_agg.piecesInDisplayDispenser,
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
# MAGIC   AND itemType in ('FINISHED GOODS', 'ACCESSORIES')

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW g_apac_fts.fts_products_org_v
# MAGIC as
# MAGIC select
# MAGIC   item_ID,
# MAGIC   productStatus,
# MAGIC   organization_ID,
# MAGIC   commodityCode 
# MAGIC from s_core.product_org_agg
# MAGIC   where _SOURCE = 'EBS'
# MAGIC   AND not _DELETED

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW g_apac_fts.fts_products_origin_v as 
# MAGIC select
# MAGIC   item_ID,
# MAGIC   itemId,
# MAGIC   origin_ID,
# MAGIC   originId,
# MAGIC   commodityCode,
# MAGIC   primaryOrigin,
# MAGIC   organizationName
# MAGIC from s_core.product_origin_agg
# MAGIC   where _SOURCE = 'EBS'
# MAGIC   AND not _DELETED

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW g_apac_fts.fts_operating_units_v
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
# MAGIC CREATE OR REPLACE VIEW g_apac_fts.fts_inventory_units_v
# MAGIC AS SELECT
# MAGIC _ID,
# MAGIC name,
# MAGIC organizationCode,
# MAGIC organizationId
# MAGIC from s_core.organization_agg
# MAGIC WHERE _SOURCE = 'EBS'
# MAGIC AND organizationType = 'INV'
# MAGIC AND not _DELETED

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW g_apac_fts.fts_accounts_v AS
# MAGIC select
# MAGIC   _ID,
# MAGIC   accountId,
# MAGIC   accountNumber,
# MAGIC   name
# MAGIC from
# MAGIC   s_core.account_agg
# MAGIC where
# MAGIC   account_agg._source = 'EBS'
# MAGIC   and not account_agg._deleted

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW g_apac_fts.fts_origin_v AS
# MAGIC select
# MAGIC   _ID,
# MAGIC   ansellPlant,
# MAGIC   originId,
# MAGIC   originName
# MAGIC from
# MAGIC   s_core.origin_agg
# MAGIC where
# MAGIC   origin_agg._source = 'EBS'
# MAGIC   and not _deleted

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW g_apac_fts.fts_inventory_v AS
# MAGIC select
# MAGIC Inventory.item_ID,
# MAGIC Inventory.inventoryWarehouse_ID,
# MAGIC Inventory.origin_ID,
# MAGIC Inventory.owningBusinessUnit_ID,
# MAGIC Inventory.subInventoryCode,
# MAGIC Inventory.primaryQty,
# MAGIC Inventory.ansStdQty,
# MAGIC round((Inventory.stdCostPerUnitPrimary ) * Inventory.primaryQty, 7) AS onHandSeeThruCost,
# MAGIC Inventory.glPeriodCode,
# MAGIC Inventory.inventoryDate,
# MAGIC Inventory.currency,
# MAGIC Inventory.exchangeRate,
# MAGIC Inventory.ansStdUomCode,
# MAGIC Inventory.primaryUOMCode,
# MAGIC Inventory.lotNumber,
# MAGIC Inventory.lotExpirationDate
# MAGIC from
# MAGIC s_supplychain.inventory_agg Inventory
# MAGIC where
# MAGIC Inventory._source = 'EBS'
# MAGIC and not Inventory._deleted
# MAGIC and inventorydate = (select max(inventorydate) from s_supplychain.inventory_agg Inventory where _source = 'EBS')
