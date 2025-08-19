# Databricks notebook source
# MAGIC %run ../_SHARED/FUNCTIONS/1.3/bootstrap

# COMMAND ----------

# MAGIC %run ./_SHARED/glpi_functions

# COMMAND ----------

# INPUT PARAMETERS
database_name = get_input_param('database_name', default_value = 's_ticketsystems')
overwrite = get_input_param('overwrite', 'bool', default_value = True)
sampling = get_input_param('sampling', 'bool', default_value = False)
table_name = get_input_param('table_name', default_value = 'dim_support_group_glpi')
target_folder = get_input_param('target_folder', default_value = '/datalake_silver/ticketsystems/full_data')

# COMMAND ----------

# EXTRACT
glpi_groups = spark.table('glpi.glpi_groups')
glpi_groups.createOrReplaceTempView("glpi_groups")

# COMMAND ----------

main_df = spark.sql("""
  SELECT 
    DISTINCT id AS SupportGroupNK, 
    groups_id AS ParentSupportGroupNK, 
    name AS Name, 
    completename AS FullName, 
    level AS Level
 FROM glpi_groups
 --WHERE is_usergroup = 1
 """)

# COMMAND ----------

main_f = (
  main_df
    .transform(replace_nulls_with_unknown(['Name', 'FullName']))
    .transform(replace_empty_with_unknown(['Name', 'FullName']))
    .transform(attach_source_column('GLPI'))
    .transform(attach_deleted_flag())
    .transform(attach_modified_date())
    .transform(attach_surrogate_key(['SupportGroupNK', '_SOURCE'], 'SupportGroupID'))
    .transform(attach_unknown_record(options = {'key_columns': ['SupportGroupID']}))
)
main_f.display()

# COMMAND ----------

# VALIDATE
check_distinct_count(main_f, ['SupportGroupID'])

# COMMAND ----------

 # LOAD
f_table_name = f'{database_name}.{table_name}'
register_hive_table(main_f, f_table_name, target_folder, options = {'overwrite': overwrite})
merge_into_table(main_f, f_table_name, 'SupportGroupID', options = {'auto_merge': True})  

# COMMAND ----------

# HANDLE DELETE
full_keys_f = (
  spark.table('glpi.glpi_groups')
  .filter('_DELETED IS FALSE')
  .select('id')
  .transform(attach_source_column('GLPI'))
  .transform(attach_surrogate_key(['id', '_SOURCE'], 'SupportGroupID'))
  .transform(attach_unknown_record(options = {'key_columns': ['SupportGroupID']}))
  .select('SupportGroupID')
)

apply_soft_delete(full_keys_f, f_table_name, key_columns = 'SupportGroupID')

# COMMAND ----------


