# Databricks notebook source
# MAGIC %run ../_SHARED/FUNCTIONS/1.3/bootstrap

# COMMAND ----------

# MAGIC %run ./_SHARED/glpi_functions

# COMMAND ----------

# INPUT PARAMETERS
database_name = get_input_param('database_name', default_value = 's_ticketsystems')
overwrite = get_input_param('overwrite', 'bool', default_value = True)
sampling = get_input_param('sampling', 'bool', default_value = False)
table_name = get_input_param('table_name', default_value = 'dim_customer_glpi')
target_folder = get_input_param('target_folder', default_value = '/datalake_silver/ticketsystems/full_data')

# COMMAND ----------

# EXTRACT
glpi_users = spark.table('glpi.glpi_users')
glpi_users.createOrReplaceTempView("glpi_users")

# COMMAND ----------

main_df = spark.sql("""
  SELECT 
    DISTINCT u.id AS CustomerNK, 
    u.name AS CustomerLogin, 
    u.firstname AS CustomerName, 
    u.realname AS CustomerSurname, 
    l.completename AS Location
  FROM glpi_users u
  INNER JOIN glpi.glpi_tickets_users tu ON u.id = tu.users_id AND tu.type = 1
  LEFT JOIN glpi.glpi_locations l ON u.locations_id = l.id
""")

# COMMAND ----------

main_f = (
  main_df
    .transform(lower_column("CustomerLogin"))
    .transform(explode_location)
    .transform(rename_region)
    .transform(rename_country)
    .transform(rename_city)
    .transform(replace_nulls_with_unknown(['CustomerLogin', 'CustomerName', 'CustomerSurname', 'Region', 'Country', 'City']))
    .transform(replace_empty_with_unknown(['CustomerLogin', 'CustomerName', 'CustomerSurname', 'Region', 'Country', 'City']))
    .withColumnRenamed('Region', 'CustomerRegion')
    .withColumnRenamed('Country', 'CustomerCountry')
    .withColumnRenamed('City', 'CustomerCity')
    .transform(attach_source_column('GLPI'))
    .transform(attach_deleted_flag())
    .transform(attach_modified_date())
    .transform(attach_surrogate_key(['CustomerNK', '_SOURCE'], 'CustomerID'))
    .transform(attach_unknown_record(options = {'key_columns': ['CustomerID']}))
)

main_f.display()

# COMMAND ----------

# VALIDATE
check_distinct_count(main_f, ['CustomerID'])

# COMMAND ----------

 # LOAD
f_table_name = f'{database_name}.{table_name}'
register_hive_table(main_f, f_table_name, target_folder, options = {'overwrite': overwrite})
merge_into_table(main_f, f_table_name, 'CustomerID', options = {'auto_merge': True})  

# COMMAND ----------

# HANDLE DELETE
full_keys_f = (
  spark.table('glpi.glpi_users')
  .filter('_DELETED IS FALSE')
  .select('id')
  .transform(attach_source_column('GLPI'))
  .transform(attach_surrogate_key(['id', '_SOURCE'], 'CustomerID'))
  .transform(attach_unknown_record(options = {'key_columns': ['CustomerID']}))
  .select('CustomerID')
)

apply_soft_delete(full_keys_f, f_table_name, key_columns = 'CustomerID')

# COMMAND ----------


