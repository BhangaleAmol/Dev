# Databricks notebook source
# MAGIC %run ../_SHARED/FUNCTIONS/1.3/bootstrap

# COMMAND ----------

# MAGIC %run ./_SHARED/glpi_functions

# COMMAND ----------

# INPUT PARAMETERS
database_name = get_input_param('database_name', default_value = 's_ticketsystems')
overwrite = get_input_param('overwrite', 'bool', default_value = True)
sampling = get_input_param('sampling', 'bool', default_value = False)
table_name = get_input_param('table_name', default_value = 'brid_engineer_ticket_glpi')
target_folder = get_input_param('target_folder', default_value = '/datalake_silver/ticketsystems/full_data')

# COMMAND ----------

# EXTRACT
glpi_tickets_users = spark.table('glpi.glpi_tickets_users')
glpi_tickets_users.createOrReplaceTempView("glpi_tickets_users")

# COMMAND ----------

main_df = spark.sql("""
  SELECT
    IF(tu.users_id = 0, NULL, tu.users_id) AS EngineerNK,
    IF(tu.tickets_id = 0, NULL, tu.tickets_id) AS TicketNK
  FROM glpi_tickets_users tu
  LEFT JOIN glpi.glpi_tickets t ON tu.tickets_id = t.id
  WHERE tu.type = 2 AND t.date_creation >= '2018-01-01'
""")

# COMMAND ----------

main_f = (
  main_df
    .distinct()
    .transform(attach_source_column('GLPI'))
    .transform(attach_deleted_flag())
    .transform(attach_modified_date())
    .transform(attach_surrogate_key(['EngineerNK', '_SOURCE'], 'EngineerID'))
    .transform(attach_surrogate_key(['TicketNK', '_SOURCE'], 'TicketID'))
    .drop('EngineerNK', 'TicketNK')
)

main_f.display()

# COMMAND ----------

 # LOAD
f_table_name = f'{database_name}.{table_name}'
register_hive_table(main_f, f_table_name, target_folder, options = {'overwrite': overwrite})
merge_into_table(main_f, f_table_name, ['EngineerID', 'TicketID'], options = {'auto_merge': True})  

# COMMAND ----------

# HANDLE DELETE
full_keys_f = (
  spark.table('glpi.glpi_tickets_users')
  .filter('_DELETED IS FALSE')
  .transform(attach_source_column('GLPI'))
  .transform(attach_surrogate_key(['users_id', '_SOURCE'], 'EngineerID'))
  .transform(attach_surrogate_key(['tickets_id', '_SOURCE'], 'TicketID'))
  .select('EngineerID', 'TicketID')
)

apply_soft_delete(full_keys_f, f_table_name, key_columns = ['EngineerID', 'TicketID'])

# COMMAND ----------


