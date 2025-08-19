# Databricks notebook source
# MAGIC %run ../_SHARED/FUNCTIONS/1.3/bootstrap

# COMMAND ----------

# MAGIC %run ../_SHARED/PARTIALS/1.3/header_gold_notebook

# COMMAND ----------

# VARIABLES
table_name = get_table_name(database_name, table_name)

# COMMAND ----------

# EXTRACT
source_table = 's_core.product_agg'
if incremental:
  cutoff_value = get_cutoff_value(table_name, source_table, prune_days)
  product_agg = load_incremental_dataset(source_table, '_MODIFIED', cutoff_value)
else:
  product_agg = load_full_dataset(source_table)

product_agg.createOrReplaceTempView('product_agg')

# COMMAND ----------

# SAMPLING
if sampling:
  product_agg = product_agg.limit(10)
  product_agg.createOrReplaceTempView('product_agg')

# COMMAND ----------

main_1 = spark.sql("""
select
  pa._ID Product_GUID,
  "" Image, 
  pa.productBrand Brand,
  pa.itemClass Item_Class,
  pa.gbu Product_GBU,
  pa.productCode Internal_Item_Id,
  pa.marketingCode External_Item_Id,
  pa.productStyle Style_Item_Reference,
  pa.productSubBrand Sub_Brand,
  mr.item_status Status,
  pa.name Product_Name,
  pa.productSbu Product_SBU,
  pa.sizeDescription Size,
  pa._deleted is_active,
  pa._SOURCE,
  pa._MODIFIED Updated_On
from
  product_agg pa
  join pdh.master_records mr on pa.itemId = mr.item
  where pa._SOURCE = 'PDH' and upper(mr.item_status) <> 'NEW' and upper(pa.productCode) not like 'DEL-%'
""")
main_1.display()

# COMMAND ----------

main_2 = (
  main_1
  .transform(attach_deleted_flag())
  .transform(attach_modified_date())
  #.transform(attach_source_column('TAA'))
)

# COMMAND ----------

# drop_table('g_upi._map_upi_product_master')

# COMMAND ----------

# MAP TABLE
nk_columns = ['Product_GUID', '_SOURCE']

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
main_f.display()

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

# COMMAND ----------

# MAGIC %run ../_SHARED/PARTIALS/1.3/footer_gold_notebook
