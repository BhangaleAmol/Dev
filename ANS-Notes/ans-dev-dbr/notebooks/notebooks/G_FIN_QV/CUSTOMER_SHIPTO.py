# Databricks notebook source
# MAGIC %run ../_SHARED/FUNCTIONS/1.3/bootstrap

# COMMAND ----------

# MAGIC %run ../_SCHEMA/g_fin_qv.customer_shipto

# COMMAND ----------

# INPUT
database_name = get_input_param('database_name', default_value = 'g_fin_qv')
incremental = get_input_param('incremental', 'bool', default_value = False)
key_columns = get_input_param('key_columns', 'string', default_value = 'CustShipto')
overwrite = get_input_param('overwrite', 'bool', default_value = False)
prune_days = get_input_param('prune_days', 'int', default_value = 30)
sampling = get_input_param('sampling', 'bool', default_value = False)
table_name = get_input_param('table_name', default_value = 'customer_shipto')
target_folder = get_input_param('target_folder', default_value = '/datalake_gold/fin_qv/full_data')

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
source_table = 's_core.customer_location_ebs'
if incremental:
  cutoff_value = get_cutoff_value(target_table, source_table, prune_days)  
  customer_location_ebs = load_incremental_dataset(source_table, '_MODIFIED', cutoff_value)
else:
  customer_location_ebs = load_full_dataset(source_table)
  
customer_location_ebs.createOrReplaceTempView('customer_location_ebs')
customer_location_ebs.display()

# COMMAND ----------

main = spark.sql("""
select
  distinct loc.partySiteNumber as ShipToAddressID,
  acc.accountNumber as customerID,
  acc.accountNumber || '-' || loc.partySiteNumber||'-'||CASE
    WHEN loc.siteCategory = 'LA' THEN 'LA'
    ELSE 'NA'
  END as CustShipto,
  acc.name as CustomerName,
  loc.addressLine1 as AddressLine1,
  loc.addressLine2 as AddressLine2,
  loc.city as City,
  loc.postalCode as PostalCode,
  loc.stateName as StateProvince,
  terr.territoryShortName as Country,
  CASE
    WHEN loc.siteCategory = 'LA' THEN 'LA'
    ELSE 'NA'
  END as Region,
  acc.customerDivision as Division,
  acc.industry as CustomerIndustry,
  acc.subIndustry as CustomerSubIndustry,
  acc.vertical AS Vertical
from
  customer_location_ebs loc,
  s_core.account_ebs acc,
  s_core.territory_ebs terr
where
  acc._id = loc.account_id
  and loc.territory_ID = terr._id
  and acc.customerType = 'External'
  and not acc._deleted
  and not loc._deleted
  and not terr._deleted
order by
  2""")

# COMMAND ----------

# TRANSFORM DATA
main_f = (
   main
   .transform(attach_partition_column("ShipToAddressID"))
   .transform(attach_modified_date())
   .transform(attach_deleted_flag())
   .transform(apply_schema(schema))
)

if incremental:
  main_f.display()

# COMMAND ----------

# LOAD
options = {'overwrite': overwrite, 'partition_column': '_PART'}
register_hive_table(main_f, target_table, target_folder, options = options)
merge_into_table(main_f, target_table, key_columns, options = {'auto_merge': True})

# COMMAND ----------

# UPDATE CUTOFF VALUE
if not sampling:
  cutoff_value = get_max_value(main_f, '_MODIFIED')
  update_cutoff_value(cutoff_value, target_table, 's_core.customer_location_ebs')
  update_run_datetime(run_datetime, target_table, 's_core.customer_location_ebs')
