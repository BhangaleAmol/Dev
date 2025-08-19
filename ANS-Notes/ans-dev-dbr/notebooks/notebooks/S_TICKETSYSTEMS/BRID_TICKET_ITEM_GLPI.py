# Databricks notebook source
# MAGIC %run ../_SHARED/FUNCTIONS/1.3/bootstrap

# COMMAND ----------

# MAGIC %run ./_SHARED/glpi_functions

# COMMAND ----------

# INPUT PARAMETERS
database_name = get_input_param('database_name', default_value = 's_ticketsystems')
overwrite = get_input_param('overwrite', 'bool', default_value = True)
sampling = get_input_param('sampling', 'bool', default_value = False)
table_name = get_input_param('table_name', default_value = 'brid_ticket_item_glpi')
target_folder = get_input_param('target_folder', default_value = '/datalake_silver/ticketsystems/full_data')

# COMMAND ----------

# EXTRACT
glpi_items_tickets = spark.table('glpi.glpi_items_tickets')
glpi_items_tickets.createOrReplaceTempView("glpi_items_tickets")

# COMMAND ----------

main_df = spark.sql("""
  SELECT
    IF(it.tickets_id = 0, NULL, it.tickets_id) AS TicketNK,
    IF(it.items_id = 0, NULL, it.items_id) AS ItemNK,
    it.ItemType,
    it._DELETED
  FROM glpi_items_tickets it
  LEFT JOIN glpi.glpi_tickets t ON it.tickets_id = t.id
  WHERE t.date_creation >= '2018-01-01'
""")

# COMMAND ----------

main_f = (
  main_df
    .distinct()
    .transform(attach_source_column('GLPI'))
    .transform(attach_deleted_flag())
    .transform(attach_modified_date())
    .transform(attach_surrogate_key(['TicketNK', '_SOURCE'], 'TicketID'))
    .transform(attach_surrogate_key(['ItemNK', '_SOURCE'], 'ItemID'))
)

main_f.display()

# COMMAND ----------

 # LOAD
f_table_name = f'{database_name}.{table_name}'
register_hive_table(main_f, f_table_name, target_folder, options = {'overwrite': overwrite})
merge_into_table(main_f, f_table_name, ['TicketID', 'ItemID'], options = {'auto_merge': True})  

# COMMAND ----------

# HANDLE DELETE
full_keys_f = (
  spark.table('glpi.glpi_items_tickets')
  .filter('_DELETED IS FALSE')
  .transform(attach_source_column('GLPI'))
  .transform(attach_surrogate_key(['tickets_id', '_SOURCE'], 'TicketID'))
  .transform(attach_surrogate_key(['items_id', '_SOURCE'], 'ItemID'))
  .select('TicketID', 'ItemID')
)

apply_soft_delete(full_keys_f, f_table_name, key_columns = ['TicketID', 'ItemID'])
