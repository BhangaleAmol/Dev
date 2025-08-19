# Databricks notebook source
# MAGIC %sql
# MAGIC create database if not exists g_quality

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW g_quality.product_region_v AS
# MAGIC SELECT 
# MAGIC case when prg.GBU in ('INDUSTRIAL', 'SINGLE USE') then 'IGBU'
# MAGIC when prg .GBU in ('MEDICAL') then 'HGBU'
# MAGIC else 'Undefined' END  GBU,
# MAGIC case when prg .REGION in ('EMEA', 'APAC')  then prg.REGION
# MAGIC when prg .REGION in ('NALAC')  then 'NA'
# MAGIC else 'Undefined' END REGION,
# MAGIC prg.productSbu SBU,
# MAGIC prg.itemId GPID,
# MAGIC prg.productStyle STYLE_CODE,
# MAGIC prg.name NAME,
# MAGIC prg.productBrand BRAND,
# MAGIC prg.sizeDescription SIZE,
# MAGIC prg.ansStdUom STANDARD_UOM,
# MAGIC prg.piecesinCase PIECES_IN_CASE,
# MAGIC prg.caseWidth  CASE_WIDTH,
# MAGIC prg.caseHeight CASE_HEIGHT,
# MAGIC prg.caseLength CASE_LENGTH,
# MAGIC prg.casenetWeight CASE_NET_WEIGHT,
# MAGIC prg.caseGrossWeight CASE_GROSS_WEIGHT,
# MAGIC prg.caseVolume VOLUME,
# MAGIC prg.productCode PRODUCTCODE
# MAGIC from s_core.product_region_agg prg
# MAGIC where _SOURCE = 'MDM'
# MAGIC and not _DELETED

# COMMAND ----------


