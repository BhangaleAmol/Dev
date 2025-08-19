# Databricks notebook source
# MAGIC %run ../_SHARED/FUNCTIONS/1.3/bootstrap

# COMMAND ----------

# MAGIC %run ./_SHARED/glpi_functions

# COMMAND ----------

# INPUT PARAMETERS
database_name = get_input_param('database_name', default_value = 's_ticketsystems')
overwrite = get_input_param('overwrite', 'bool', default_value = True)
sampling = get_input_param('sampling', 'bool', default_value = False)
table_name = get_input_param('table_name', default_value = 'dim_ticket_glpi')
target_folder = get_input_param('target_folder', default_value = '/datalake_silver/ticketsystems/full_data')

# COMMAND ----------

# EXTRACT
glpi_tickets = spark.table('glpi.glpi_tickets')
glpi_tickets.createOrReplaceTempView("glpi_tickets")

# COMMAND ----------

ticketsatisfactions_f = spark.sql("""
  WITH satisfaction AS
  (
    SELECT 
      tickets_id,
      satisfaction,
      ROW_NUMBER() OVER (PARTITION BY tickets_id ORDER BY _MODIFIED DESC) AS RowNum
    FROM glpi.glpi_ticketsatisfactions
  )
  SELECT tickets_id, satisfaction FROM satisfaction WHERE RowNum = 1
""")

ticketsatisfactions_f.createOrReplaceTempView('ticketsatisfactions_f')

# COMMAND ----------

main_df = spark.sql("""
  SELECT 
    t.id, 
    t.name, 
    t.priority, 
    t.urgency, 
    t.impact, 
    t.type, 
    rt.name AS Source, 
    t.status, 
    ts.satisfaction, 
    t.content, 
    NVL(CAST(t.is_deleted AS BOOLEAN), False) AS is_deleted,
    sl.name AS sla
  FROM glpi_tickets t
  LEFT JOIN glpi.glpi_requesttypes rt ON t.requesttypes_id = rt.id
  LEFT JOIN ticketsatisfactions_f ts ON t.id = ts.tickets_id
  LEFT JOIN glpi.glpi_slas sl ON sl.id = t.slas_id_ttr
  WHERE t.date_creation >= '2018-01-01'
""")
main_df.display()

# COMMAND ----------

main_f = (
  main_df
    .withColumn("satisfaction", f.col("satisfaction").cast(StringType()))
    .withColumn("is_deleted", f.col("is_deleted").cast(BooleanType()))
    .transform(translate_priority)
    .transform(translate_status)
    .transform(translate_urgency)
    .transform(translate_impact)
    .transform(translate_type)
    .na.fill("Unknown")  
    .withColumnRenamed("id", "TicketNK")
    .withColumnRenamed("name", "Title")
    .withColumnRenamed("priority", "Priority")
    .withColumnRenamed("urgency", "Urgency")
    .withColumnRenamed("impact", "Impact")
    .withColumnRenamed("type", "Type")
    .withColumnRenamed("status", "Status")
    .withColumnRenamed("satisfaction", "Satisfaction")
    .withColumnRenamed("content", "Content")
    .withColumnRenamed("is_deleted", "IsDeleted")
    .withColumnRenamed("sla", "Sla")
    .transform(attach_source_column('GLPI'))
    .transform(attach_deleted_flag())
    .transform(attach_modified_date())
    .transform(attach_surrogate_key(['TicketNK', '_SOURCE'], 'TicketID'))
    .transform(attach_unknown_record(options = {'key_columns': ['TicketID']}))
)

main_f.display()

# COMMAND ----------

# VALIDATE
check_distinct_count(main_f, ['TicketID'])

# COMMAND ----------

 # LOAD
f_table_name = f'{database_name}.{table_name}'
register_hive_table(main_f, f_table_name, target_folder, options = {'overwrite': overwrite})
merge_into_table(main_f, f_table_name, 'TicketID', options = {'auto_merge': True})  

# COMMAND ----------

# HANDLE DELETE
full_keys_f = (
  spark.table('glpi.glpi_tickets')
  .filter('_DELETED IS FALSE')
  .select('id')
  .transform(attach_source_column('GLPI'))
  .transform(attach_surrogate_key(['id', '_SOURCE'], 'TicketID'))
  .transform(attach_unknown_record(options = {'key_columns': ['TicketID']}))
  .select('TicketID')
)

apply_soft_delete(full_keys_f, f_table_name, key_columns = 'TicketID')

# COMMAND ----------


