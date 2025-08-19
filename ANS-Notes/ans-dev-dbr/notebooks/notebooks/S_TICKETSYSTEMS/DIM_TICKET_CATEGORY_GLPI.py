# Databricks notebook source
# MAGIC %run ../_SHARED/FUNCTIONS/1.3/bootstrap

# COMMAND ----------

# MAGIC %run ./_SHARED/glpi_functions

# COMMAND ----------

# INPUT PARAMETERS
database_name = get_input_param('database_name', default_value = 's_ticketsystems')
overwrite = get_input_param('overwrite', 'bool', default_value = True)
sampling = get_input_param('sampling', 'bool', default_value = False)
table_name = get_input_param('table_name', default_value = 'dim_ticket_category_glpi')
target_folder = get_input_param('target_folder', default_value = '/datalake_silver/ticketsystems/full_data')

# COMMAND ----------

# EXTRACT
glpi_itilcategories = spark.table('glpi.glpi_itilcategories')
glpi_itilcategories.createOrReplaceTempView("glpi_itilcategories")

# COMMAND ----------

CATEGORY_DF = spark.sql("""
  SELECT 
    id AS TicketCategoryNK, 
    itilcategories_id AS ParentTicketCategoryNK, 
    name AS Name, 
    completename AS FullName 
  FROM glpi_itilcategories
""")

# COMMAND ----------

main_f = (
  CATEGORY_DF
    .transform(drop_duplicates(['TicketCategoryNK']))
    .transform(replace_nulls_with_unknown(['Name', 'FullName']))
    .transform(replace_empty_with_unknown(['Name', 'FullName']))
    .transform(attach_source_column('GLPI'))
    .transform(attach_deleted_flag())
    .transform(attach_modified_date())
    .transform(attach_surrogate_key(['TicketCategoryNK', '_SOURCE'], 'TicketCategoryID'))
    .transform(attach_surrogate_key(['ParentTicketCategoryNK', '_SOURCE'], 'ParentTicketCategoryID'))
    .transform(attach_unknown_record(options = {'key_columns': ['TicketCategoryID', 'ParentTicketCategoryID']}))
    .drop('TicketCategoryNK', 'ParentTicketCategoryNK')
)

main_f.display()

# COMMAND ----------

# VALIDATE
check_distinct_count(main_f, ['TicketCategoryID'])

# COMMAND ----------

 # LOAD
f_table_name = f'{database_name}.{table_name}'
register_hive_table(main_f, f_table_name, target_folder, options = {'overwrite': overwrite})
merge_into_table(main_f, f_table_name, 'TicketCategoryID', options = {'auto_merge': True})  

# COMMAND ----------

# HANDLE DELETE
full_keys_f = (
  spark.table('glpi.glpi_itilcategories')
  .filter('_DELETED IS FALSE')
  .select('id')
  .transform(attach_source_column('GLPI'))
  .transform(attach_surrogate_key(['id', '_SOURCE'], 'TicketCategoryID'))
  .transform(attach_unknown_record(options = {'key_columns': ['TicketCategoryID']}))
  .select('TicketCategoryID')
)

apply_soft_delete(full_keys_f, f_table_name, key_columns = 'TicketCategoryID')

# COMMAND ----------


