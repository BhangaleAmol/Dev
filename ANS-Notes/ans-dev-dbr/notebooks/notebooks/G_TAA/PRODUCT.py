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
  pa._ID,
  pa.itemId,
  pa.marketingCode,
  pa.name,
  pa.gbu,
  pa.productBrand,
  pa.productStyle,
  pa.productSubBrand,
  pa.productSbu,
  pa.itemClass,
  mr.private_label,
  po.originId,
  org.originName,
  org.ansellPlant,
  org.originLocation,
  po.primaryOrigin,
  mr.item_status
from
  product_agg pa
  join (
    select
      distinct item_ID,
      itemId,
      originId,
      origin_ID,
      primaryOrigin
    from
      s_core.product_origin_agg
    where
      _source = 'PDH'
      and _deleted is false
  ) po on pa._ID = po.item_Id
  join s_core.origin_agg org on po.origin_ID = org._ID
  join pdh.master_records mr on pa.itemId = mr.item
where
  pa._source = 'PDH'
  and pa._deleted is false
  and mr.sold_to_nalac is true
""")

# COMMAND ----------

main_2 = (
  main_1
  .transform(attach_deleted_flag())
  .transform(attach_modified_date())
  .transform(attach_source_column('TAA'))
)

# COMMAND ----------

# MAP TABLE
nk_columns = ['itemId', 'originId', '_SOURCE']

map_table = get_map_table_name(table_name)
if not table_exists(map_table):
  map_table_path = get_table_path(map_table, target_container, target_storage)
  create_map_table(map_table, map_table_path)
    
update_map_table(map_table, main_2, nk_columns)

# COMMAND ----------

main_f = (
  main_2
  .transform(attach_primary_key(nk_columns, map_table))
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
