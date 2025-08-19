# Databricks notebook source
# MAGIC %sql
# MAGIC create database if not exists g_apac_sup

# COMMAND ----------

# MAGIC %sql 
# MAGIC CREATE OR REPLACE VIEW g_apac_sup.apac_product_v AS
# MAGIC select
# MAGIC   product_agg._ID,
# MAGIC   product_agg._SOURCE,
# MAGIC   product_agg.gbu,
# MAGIC   product_agg.itemId,
# MAGIC   product_agg.productDivision,
# MAGIC   product_agg.productBrand,
# MAGIC   product_agg.productSubBrand,
# MAGIC   product_agg.productStyle,
# MAGIC   product_agg.marketingCode,
# MAGIC   product_agg.legacyASPN,
# MAGIC   product_agg.productCode,
# MAGIC   product_agg.name,
# MAGIC   product_agg.originId as originCode,
# MAGIC   product_agg.originDescription,
# MAGIC   product_agg.ansStdUomConv,
# MAGIC   product_agg.ansStdUom
# MAGIC from
# MAGIC   s_core.product_agg
# MAGIC where
# MAGIC   _source in ( 'EBS')

# COMMAND ----------

# MAGIC %sql 
# MAGIC CREATE OR REPLACE VIEW g_apac_sup.apac_organization_v AS
# MAGIC select 
# MAGIC _ID,
# MAGIC _SOURCE,
# MAGIC organizationId,
# MAGIC organizationCode,
# MAGIC name,
# MAGIC organizationType
# MAGIC from s_core.organization_agg c
# MAGIC where
# MAGIC   _source in ('EBS')

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW g_apac_sup.apac_inventory_v AS
# MAGIC select
# MAGIC   inventoryWarehouse_ID,
# MAGIC   item_id,
# MAGIC   owningBusinessUnit_ID,
# MAGIC   subInventoryCode,
# MAGIC   stdCostPerUnitPrimary,
# MAGIC   primaryQty,
# MAGIC   ansStdQty,
# MAGIC   exchangeRate,
# MAGIC   lotNumber,
# MAGIC   glPeriodcode,
# MAGIC   lotExpirationDate,
# MAGIC   _ID
# MAGIC from
# MAGIC   s_supplychain.inventory_agg
# MAGIC where
# MAGIC   _source in ('EBS')
# MAGIC   and not _deleted
