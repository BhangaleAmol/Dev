# Databricks notebook source
# MAGIC %run ../_SHARED/FUNCTIONS/1.3/bootstrap

# COMMAND ----------

# MAGIC %run ../_SHARED/PARTIALS/1.3/header_gold_notebook

# COMMAND ----------

# database_name = "g_upi" # [str]
# incremental = False # [bool]
# metadata_container = "datalake" # [str]
# metadata_folder = "/datalake/g_upi/metadata" # [str]
# metadata_storage = "edmans{env}data003" # [str]
# overwrite = False # [bool]
# prune_days = 10 # [int]
# sampling = False # [bool]
# table_name = "UPI_PRODUCT_STAGING" # [str]
# target_container = "datalake" # [str]
# target_folder = "/datalake/g_upi/full_data" # [str]
# target_storage = "edmans{env}data003" # [str]

# COMMAND ----------

# VARIABLES
table_name = get_table_name(database_name, table_name)

# COMMAND ----------

# drop_table('g_upi._map_upi_product_staging')
# drop_table('g_upi.upi_product_staging')

# COMMAND ----------

# EXTRACT
if incremental:
  cutoff_value = get_cutoff_value(table_name, 's_core.product_agg', prune_days)
  product_agg = load_incremental_dataset('s_core.product_agg', '_MODIFIED', cutoff_value)

  cutoff_value = get_cutoff_value(table_name, 'pdh.master_records', prune_days)
  master_records = load_incremental_dataset('pdh.master_records', '_MODIFIED', cutoff_value)

  cutoff_value = get_cutoff_value(table_name, 's_core.product_origin_agg', prune_days)
  product_origin_agg = load_incremental_dataset('s_core.product_origin_agg', '_MODIFIED', cutoff_value)

  cutoff_value = get_cutoff_value(table_name, 's_core.origin_agg', prune_days)
  origin_agg = load_incremental_dataset('s_core.origin_agg', '_MODIFIED', cutoff_value)
else:
  product_agg = load_full_dataset('s_core.product_agg')
  master_records = load_full_dataset('pdh.master_records')
  product_origin_agg = load_full_dataset('s_core.product_origin_agg')
  origin_agg = load_full_dataset('s_core.origin_agg')

product_agg.createOrReplaceTempView('product_agg')
master_records.createOrReplaceTempView('master_records')
product_origin_agg.createOrReplaceTempView('product_origin_agg')
origin_agg.createOrReplaceTempView('origin_agg')

# COMMAND ----------

# Product Aggregate records with related changes in PDH Master Records
products_with_mr_changed_df = spark.sql('''
  SELECT pa.* 
  FROM master_records mr 
  JOIN s_core.product_agg pa ON mr.item = pa.productCode 
''')

products_with_mr_changed_df.createOrReplaceTempView('products_with_mr_changed_df')

# COMMAND ----------

# Product Aggregate records with related changes in Product Origin Aggregate
products_with_poa_changed_df = spark.sql('''
  SELECT pa.*
  FROM product_origin_agg poa
  JOIN s_core.product_agg pa ON poa.item_ID = pa._ID and poa._SOURCE = pa._SOURCE 
''')

products_with_poa_changed_df.createOrReplaceTempView('products_with_poa_changed_df')

# COMMAND ----------

# Product Aggregate records with related changes in Origin Aggregate
products_with_oa_changed_df = spark.sql('''
  SELECT pa.*
  FROM origin_agg oa
  JOIN s_core.product_agg pa ON oa._ID = pa.origin_ID and oa._SOURCE = pa._SOURCE 
''')

products_with_oa_changed_df.createOrReplaceTempView('products_with_oa_changed_df')

# COMMAND ----------

# Create dataset for main query
products_with_changes_df = spark.sql('''
SELECT * FROM products_with_mr_changed_df
UNION 
SELECT * FROM products_with_poa_changed_df 
UNION 
SELECT * FROM products_with_oa_changed_df
''')

products_with_changes_df.createOrReplaceTempView('products_with_changes_df')

# COMMAND ----------

product_origin_id = spark.sql("""
SELECT
  distinct po.item_ID,
  po.originId,
  po.procureViaGtc,
  po.countryKnitted,
  po.countryDipped,
  po.processDescription,
  oa.originName,
  "Y" primaryOrigin,
  oa.originLocation,
  CASE
    WHEN oa.ansellPlant is TRUE THEN "Y"
    ELSE "N"
  END ansellPlant,
  po._SOURCE
FROM
  s_core.product_origin_agg po
  left join s_core.origin_agg oa on po.originId = oa.originId
WHERE
  po._DELETED is false
  and oa._DELETED is false
  and po.primaryOrigin is TRUE
  and po._SOURCE = oa._SOURCE;""")

product_origin_id.createOrReplaceTempView('product_origin_id')

# COMMAND ----------

main_1 = spark.sql("""
select
  pa.productCode Internal_Item_Id,
  pa._SOURCE DL_Src_Name,
  pa._SOURCE,
  inline(
    arrays_zip(
      array(
        'gtcId',
        'color',
        'Internal_Item_Id',
        'Brand',
        'Item_Class',
        'Product_GBU',
        'External_Item_Id',
        'Style_Item_Reference',
        'Sub_Brand',
        'Product_Name',
        'Product_SBU',
        'brandStrategy',
        'sizeDescription',
        'ansStdUom',
        'productType',
        'acquisitionName',
        'productM4Family',
        'productM4Group',
        'productM4Category',
        'productM4Segment',
        'productFamily',
        'productCategory',
        'baseStyle',
        'logilityShortDesc',
        'harmonizedSizeCode',
        'packType',
        'gpCategory',
        'baseProduct',
        'dippingPlant',
        'knittingPlant',
        'knittingGauge',
        'rePackingCode',
        'originId',
        'originName',
        'primaryOrigin',
        'originLocation',
        'ansellPlant',
        'procureViaGtc',
        'countryKnitted',
        'countryDipped',
        'processDescription',
        'ancillarySupplier',
        'gtinOuter',
        'gtinInner',
        'gtinUnit',
        'caseLength',
        'caseWidth',
        'caseHeight',
        'caseGrossWeight',
        'caseNetWeight',
        'caseVolume',
        'piecesInCarton',
        'packagingConfigurationPiece',
        'launchYear',
        'shelfLife',
        'primarySellingUom',
        'secondarySellingUom',
        'lowestShippableUom'
      ),
      array(
        CASE
          WHEN pa._SOURCE = 'EBS' THEN pa.productCode
          WHEN pa._SOURCE = 'PDH' THEN mr.ORACLE_PRODUCT_ID_OR_GTC_ID
          ELSE ""
        END,
        CASE
          WHEN pa._SOURCE = 'PDH' THEN mr.COLOR
          ELSE ""
        END,
        pa.productCode,
        pa.productBrand,
        pa.itemClass,
        pa.gbu,
        pa.marketingCode,
        pa.productStyle,
        pa.productSubBrand,
        pa.name,
        pa.productSbu,
        pa.brandStrategy,
        CASE
          WHEN pa._SOURCE = 'PDH' THEN mr.size
          ELSE pa.sizeDescription
        END,
        CASE
          WHEN pa.ansStdUom = "PAA" THEN "PAIR"
          WHEN pa.ansStdUom = "ST" THEN "PIECE"
          WHEN pa.ansStdUom = "PC" THEN "PIECE"
          WHEN pa.ansStdUom = "PR" THEN "PAIR"
          WHEN pa.ansStdUom = "GR" THEN "GROSS"
          ELSE pa.ansStdUom
        END,
        CASE
          WHEN pa.productType = "ZPRF" THEN "Finished Good"
          ELSE pa.productType
        END,
        pa.acquisitionName,
        pa.productM4Family,
        pa.productM4Group,
        pa.productM4Category,
        pa.productM4Segment,
        pa.productFamily,
        pa.productCategory,
        CASE
          WHEN pa._SOURCE = 'PDH' THEN mr.BASE_STYLE
          ELSE ""
        END,
        CASE
          WHEN pa._SOURCE = 'PDH' THEN mr.LOGILITY_SHORT_DESCRIPTION
          ELSE ""
        END,
        CASE
          WHEN pa._SOURCE = 'PDH' THEN mr.HARMONIZED_SIZE_CODE
          ELSE ""
        END,
        CASE
          WHEN pa._SOURCE = 'PDH' THEN mr.PACK_TYPE
          ELSE ""
        END,
        CASE
          WHEN pa._SOURCE = 'PDH' THEN mr.GP_CATEGORY
          ELSE ""
        END,
        CASE
          WHEN pa._SOURCE = 'PDH' THEN mr.BASE_PRODUCT
          ELSE ""
        END,
        CASE
          WHEN pa._SOURCE = 'PDH' THEN mr.DIPPING_PLANT
          ELSE ""
        END,
        CASE
          WHEN pa._SOURCE = 'PDH' THEN mr.KNITTING_PLANT
          ELSE ""
        END,
        CASE
          WHEN pa._SOURCE = 'PDH' THEN ""
          ELSE ""
        END,
        pa.rePackingCode,
        poa.originId,
        poa.originName,
        CASE
          WHEN poa.primaryOrigin is TRUE THEN "Y"
          ELSE "N"
        END,
        poa.originLocation,
        CASE
          WHEN poa.ansellPlant is TRUE THEN "Y"
          ELSE "N"
        END,
        poa.procureViaGtc,
        poa.countryKnitted,
        poa.countryDipped,
        poa.processDescription,
        "",
        pa.gtinOuter,
        pa.gtinInner,
        pa.gtinUnit,
        CAST(pa.caseLength AS STRING),
        CAST(pa.caseWidth AS STRING),
        CAST(pa.caseHeight AS STRING),
        CAST(pa.caseGrossWeight AS STRING),
        CAST(pa.caseNetWeight AS STRING),
        CAST(pa.caseVolume AS STRING),
        CAST(pa.piecesInCarton AS STRING),
        CASE
          WHEN pa._SOURCE = 'PDH' THEN mr.PACKAGING_CONFIGURATION_PIECE
          ELSE ""
        END,
        pa.productLaunchYear,
        pa.shelfLife,
        CASE
          WHEN pa.primarySellingUom = "CA" THEN "CASE"
          WHEN pa.primarySellingUom = "ST" THEN "PIECE"
          WHEN pa.primarySellingUom = "PAA" THEN "PAIR"
          ELSE pa.primarySellingUom
        END,
        CASE
          WHEN pa.secondarySellingUom = "DZ" THEN "DOZEN"
          WHEN pa.secondarySellingUom = "EA" THEN "EACH"
          WHEN pa.secondarySellingUom = "PC" THEN "PIECE"
          WHEN pa.secondarySellingUom = "PR" THEN "PAIR"
          WHEN pa.secondarySellingUom = "BX" THEN "BOX"
          WHEN pa.secondarySellingUom = "PK" THEN "PACK"
          WHEN pa.secondarySellingUom = "CA" THEN "CASE"
          WHEN pa.secondarySellingUom = "CT" THEN "CARTON"
          WHEN pa.secondarySellingUom = "KT" THEN "KIT"
          WHEN pa.secondarySellingUom = "BG" THEN "BAG"
          WHEN pa.secondarySellingUom = "RL" THEN "ROLL"
          WHEN pa.secondarySellingUom = "DP" THEN "DISPENSER"
          WHEN pa.secondarySellingUom = "PAA" THEN "PAIR"
          WHEN pa.secondarySellingUom = "ST" THEN "PIECE"
          ELSE pa.secondarySellingUom
        END,
        CASE
          WHEN pa.lowestShippableUom = "PAA" THEN "PAIR"
          WHEN pa.lowestShippableUom = "ST" THEN "PIECE"
          WHEN pa.lowestShippableUom = "BG" THEN "BAG"
          WHEN pa.lowestShippableUom = "PR" THEN "PAIR"
          ELSE pa.lowestShippableUom
        END,
        pa.primarySellingUom,
        pa.secondarySellingUom,
        pa.lowestShippableUom
      )
    )
  ) AS (DL_Attr_Col_Name, Product_Information)
FROM
  s_core.product_agg pa
  join pdh.master_records mr on pa.productCode = mr.item
  left join product_origin_id poa on pa._ID = poa.item_ID
  and pa._SOURCE = poa._SOURCE
WHERE
  pa._deleted is false and pa._SOURCE in ('PDH', 'EBS', 'SAP', 'SF', 'QV');
""")

# main_1.display()

# COMMAND ----------

main_2 = (
  main_1
  .transform(attach_deleted_flag())
  .transform(attach_modified_date())
  #.transform(attach_source_column('TAA'))
)

# COMMAND ----------

# MAP TABLE
nk_columns = ['Internal_Item_Id', 'DL_Attr_Col_Name', '_SOURCE']

map_table = get_map_table_name(table_name)
if not table_exists(map_table):
  map_table_path = get_table_path(map_table, target_container, target_storage)
  create_map_table(map_table, map_table_path)
    
update_map_table(map_table, main_2, nk_columns)

# COMMAND ----------

main_f = (
  main_2
  .transform(attach_primary_key(nk_columns, map_table, '_ID'))
  .transform(sort_columns)
)

main_f.createOrReplaceTempView('main_f')
# main_f.display()

# COMMAND ----------

# LOAD
options = {
  'overwrite': overwrite, 
  'storage_name': target_storage, 
  'container_name': target_container
}
register_hive_table(main_f, table_name, target_folder, options)
merge_into_table(main_f, table_name, ['_ID'])

# COMMAND ----------

# UPDATE CUTOFF VALUE
if not sampling:
  cutoff_value = get_max_value(product_agg, '_MODIFIED')
  update_cutoff_value(cutoff_value, table_name, 's_core.product_agg')
  update_run_datetime(run_datetime, table_name, 's_core.product_agg')

  cutoff_value = get_max_value(master_records, '_MODIFIED')
  update_cutoff_value(cutoff_value, table_name, 'pdh.master_records')
  update_run_datetime(run_datetime, table_name, 'pdh.master_records')

  cutoff_value = get_max_value(product_origin_agg, '_MODIFIED')
  update_cutoff_value(cutoff_value, table_name, 's_core.product_origin_agg')
  update_run_datetime(run_datetime, table_name, 's_core.product_origin_agg')

  cutoff_value = get_max_value(origin_agg, '_MODIFIED')
  update_cutoff_value(cutoff_value, table_name, 's_core.origin_agg')
  update_run_datetime(run_datetime, table_name, 's_core.origin_agg')

# COMMAND ----------

# MAGIC %run ../_SHARED/PARTIALS/1.3/footer_gold_notebook
