# Databricks notebook source
# MAGIC %run ../_SHARED/FUNCTIONS/1.3/bootstrap

# COMMAND ----------

# MAGIC %run ../_SHARED/PARTIALS/1.3/header_gold_notebook

# COMMAND ----------

# database_name = "g_upi" # [str]
# incremental = True # [bool]
# metadata_container = "datalake" # [str]
# metadata_folder = "/datalake/g_upi/metadata" # [str]
# metadata_storage = "edmans{env}data003" # [str]
# overwrite = False # [bool]
# prune_days = 10 # [int]
# sampling = False # [bool]
# table_name = "UPI_PRODUCT_CUSTOMS" # [str]
# target_container = "datalake" # [str]
# target_folder = "/datalake/g_upi/full_data" # [str]
# target_storage = "edmans{env}data003" # [str]

# COMMAND ----------

# VARIABLES
table_name = get_table_name(database_name, table_name)

# COMMAND ----------

# drop_table('g_upi._map_upi_product_customs')
# drop_table('g_upi.upi_product_customs')

# COMMAND ----------

# EXTRACT
source_table = 'pdh.customs_codes'
if incremental:
  cutoff_value = get_cutoff_value(table_name, source_table, prune_days)
  customs_codes = load_incremental_dataset(source_table, '_MODIFIED', cutoff_value)
else:
  customs_codes = load_full_dataset(source_table)

customs_codes.createOrReplaceTempView('customs_codes')

# COMMAND ----------

# SAMPLING
if sampling:
  customs_codes = customs_codes.limit(10)
  customs_codes.createOrReplaceTempView('customs_codes')

# COMMAND ----------

main_1 = spark.sql("""
select
  item Internal_Item_Id,
  organization_name Region,
  sos SOS,
  sub_region Sub_Region,
  country_of_origin Country_Of_Origin,
  CASE
    WHEN organization_name = 'NALAC' THEN hts
    ELSE commodity_code
  END Commodity_Code,
  "" Dual_Use,
  "" Schedule_B,
  CASE
    WHEN _DELETED is true THEN '0'
    ELSE '1'
  END Is_Active,
  _MODIFIED Updated_On,
  "PDH" _SOURCE
FROM
  pdh.customs_codes
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
nk_columns = ['Internal_Item_Id', '_SOURCE', 'Sub_Region', 'SOS']

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
  cutoff_value = get_max_value(customs_codes, '_MODIFIED')
  update_cutoff_value(cutoff_value, table_name, 'pdh.customs_codes')
  update_run_datetime(run_datetime, table_name, 'pdh.customs_codes')

# COMMAND ----------

# MAGIC %run ../_SHARED/PARTIALS/1.3/footer_gold_notebook
