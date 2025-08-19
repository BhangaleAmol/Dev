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
# table_name = "UPI_PRODUCT_REGION" # [str]
# target_container = "datalake" # [str]
# target_folder = "/datalake/g_upi/full_data" # [str]
# target_storage = "edmans{env}data003" # [str]

# COMMAND ----------

# drop_table('g_upi._map_upi_product_region')
# drop_table('g_upi.upi_product_region')

# COMMAND ----------

# VARIABLES
table_name = get_table_name(database_name, table_name)

# COMMAND ----------

# EXTRACT
source_table = 'pdh.regional'
if incremental:
  cutoff_value = get_cutoff_value(table_name, source_table, prune_days)
  regional = load_incremental_dataset(source_table, '_MODIFIED', cutoff_value)
else:
  regional = load_full_dataset(source_table)

regional.createOrReplaceTempView('regional')

# COMMAND ----------

main_1 = spark.sql("""
select
  item Internal_Item_Id,
  "Region_Americas" Region,
  "PDH" _SOURCE,
  _MODIFIED Updated_On,
  CASE
    WHEN _DELETED is true THEN '0'
    ELSE '1'
  END Is_active,
  inline(
    arrays_zip(
      array(
        'USA',
        'Canada',
        'LAC',
        'Brazil',
        'Colombia - DNAV'
      ),
      array(
        CASE
          WHEN sub_region_usa is null THEN 'N'
          ELSE sub_region_usa
        END,
        CASE
          WHEN sub_region_canada is null THEN 'N'
          ELSE sub_region_canada
        END,
        CASE
          WHEN sub_region_lac is null THEN 'N'
          ELSE sub_region_lac
        END,
        CASE
          WHEN sub_region_brazil is null THEN 'N'
          ELSE sub_region_brazil
        END,
        CASE
          WHEN sub_region_colombia is null THEN 'N'
          ELSE sub_region_colombia
        END
      )
    )
  ) AS (Country, isAllocated)
FROM
  pdh.regional where organization_name = 'NALAC'
UNION
select
  item Internal_Item_Id,
  "Region_EMEA" Region,
  "PDH" _SOURCE,
  _MODIFIED Updated_On,
  CASE
    WHEN _DELETED is true THEN '0'
    ELSE '1'
  END Is_Active,
  inline(
    arrays_zip(
      array(
        'EMEA',
        'Russia',
        'Sweden',
        'France',
        'Nitritex'
      ),
      array(
        CASE
          WHEN sub_region_emea is null THEN 'N'
          ELSE sub_region_emea
        END,
        CASE
          WHEN sub_region_russia is null THEN 'N'
          ELSE sub_region_russia
        END,
        CASE
          WHEN sub_region_sweden is null THEN 'N'
          ELSE sub_region_sweden
        END,
        CASE
          WHEN sub_region_france is null THEN 'N'
          ELSE sub_region_france
        END,
        CASE
          WHEN sub_region_nitritex is null THEN 'N'
          ELSE sub_region_nitritex
        END
      )
    )
  ) AS (Country, isAllocated)
FROM
  pdh.regional where organization_name = 'EMEA'
UNION
select
  item Internal_Item_Id,
  "Region_APAC" Region,
  "PDH" _SOURCE,
  _MODIFIED Updated_On,
  CASE
    WHEN _DELETED is true THEN '0'
    ELSE '1'
  END Is_Active,
  inline(
    arrays_zip(
      array(
        'SEA',
        'Japan',
        'China',
        'Korea',
        'India',
        'ANZ'
      ),
      array(
        CASE
          WHEN sub_region_sea is null THEN 'N'
          ELSE sub_region_sea
        END,
        CASE
          WHEN sub_region_japan is null THEN 'N'
          ELSE sub_region_japan
        END,
        CASE
          WHEN sub_region_china is null THEN 'N'
          ELSE sub_region_china
        END,
        CASE
          WHEN sub_region_korea is null THEN 'N'
          ELSE sub_region_korea
        END,
        CASE
          WHEN sub_region_india is null THEN 'N'
          ELSE sub_region_india
        END,
        CASE
          WHEN sub_region_anz is null THEN 'N'
          ELSE sub_region_anz
        END
      )
    )
  ) AS (Country, isAllocated)
FROM
  pdh.regional where organization_name = 'APAC'
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
nk_columns = ['Internal_Item_Id', '_SOURCE', 'Country']

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
  cutoff_value = get_max_value(regional, '_MODIFIED')
  update_cutoff_value(cutoff_value, table_name, 'pdh.regional')
  update_run_datetime(run_datetime, table_name, 'pdh.regional')

# COMMAND ----------

# MAGIC %run ../_SHARED/PARTIALS/1.3/footer_gold_notebook
