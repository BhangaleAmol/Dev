# Databricks notebook source
# MAGIC %run ../_SHARED/FUNCTIONS/1.3/bootstrap

# COMMAND ----------

# MAGIC %run ../_SCHEMA/g_na_tm.point_of_sales_sandel_v

# COMMAND ----------

# INPUT
database_name = get_input_param('database_name', default_value = 'g_na_tm')
incremental = False
key_columns = get_input_param('key_columns', 'list', default_value = ['_ID'])
overwrite = True
prune_days = get_input_param('prune_days', 'int', default_value = 30)
sampling = get_input_param('sampling', 'bool', default_value = False)
table_name = get_input_param('table_name', default_value = 'point_of_sales_sandel_v')
target_folder = get_input_param('target_folder', default_value = '/datalake_gold/na_tm/full_data')

# COMMAND ----------

# VALIDATE INPUT
if incremental and key_columns is None:
  raise Exception("INCREMENTAL & NO KEY COLUMNS")

if overwrite and sampling:
  raise Exception("OVERWRITE & SAMPLING")

# COMMAND ----------

# VARIABLES
target_table = get_table_name(database_name, table_name)

# COMMAND ----------

# EXTRACT
source_table = 's_trademanagement.point_of_sales_sandel'
if incremental:
  cutoff_value = get_cutoff_value(target_table, source_table, prune_days)  
  point_of_sales_sandel = load_incremental_dataset(source_table, '_MODIFIED', cutoff_value)
else:
  point_of_sales_sandel = load_full_dataset(source_table)
  
point_of_sales_sandel.createOrReplaceTempView('point_of_sales_sandel')
point_of_sales_sandel.display()

# COMMAND ----------

# SAMPLING
if sampling:
  point_of_sales_sandel = point_of_sales_sandel.limit(10)
  point_of_sales_sandel.createOrReplaceTempView('point_of_sales_sandel')

# COMMAND ----------

main_df = spark.sql("""
  select
  _id,
  createdBy,
  createdOn,
  modifiedBy,
  modifiedOn,
  insertedOn,
  updatedOn,
  brand,
  brandowner,
  brandtype,
  city,
  company,
  customertype,
  distributorid,
  distributorname,
  division,
  enduser,
  equivunit,
  familydescr,
  familyid,
  gbu,
  industry,
  materialsubtype,
  materialtype,
  postalcode,
  prodcategory,
  prodsubcategory,
  productstyle,
  qty,
  salesregion,
  skudescr,
  skuid,
  state,
  subregion,
  territory,
  tranamt,
  trandate,
  userid,
  vertical,
  osSalesRegion,
  osTerritory,
  osUserid,
  marketlevel1,
  marketlevel2,
  marketlevel3,
  org2Territory,
  org2SalesRegion,
  org2Userid
from
  s_trademanagement.point_of_sales_sandel
where
  not _DELETED
""")

main_df.cache()
main_df.display()

# COMMAND ----------

# TRANSFORM DATA
main_f = (
   main_df
   .transform(attach_partition_column("createdOn"))
   .transform(attach_modified_date())
   .transform(attach_deleted_flag())
   .transform(apply_schema(schema))
)
main_f.display()

# COMMAND ----------

# LOAD
options = {'overwrite': overwrite, 'partition_column': '_PART'}
register_hive_table(main_f, target_table, target_folder, options = options)
merge_into_table(main_f, target_table, key_columns, options = {'auto_merge': True})
