# Databricks notebook source
# MAGIC %run ../_SHARED/FUNCTIONS/1.3/bootstrap

# COMMAND ----------

# MAGIC %run ./_SHARED/glpi_functions

# COMMAND ----------

# INPUT PARAMETERS
database_name = get_input_param('database_name', default_value = 's_ticketsystems')
overwrite = get_input_param('overwrite', 'bool', default_value = True)
sampling = get_input_param('sampling', 'bool', default_value = False)
table_name = get_input_param('table_name', default_value = 'dim_engineer_glpi')
target_folder = get_input_param('target_folder', default_value = '/datalake_silver/ticketsystems/full_data')

# COMMAND ----------

# EXTRACT
glpi_tickets = spark.table('glpi.glpi_tickets')
glpi_tickets.createOrReplaceTempView('glpi_tickets')

glpi_users = spark.table('glpi.glpi_users')
glpi_users.createOrReplaceTempView('glpi_users')

# COMMAND ----------

FIRST_ENGINEER_DF = spark.sql("""
  WITH records1 AS (
    SELECT
      t.id,
      REGEXP_EXTRACT(new_value, '(\\\d+)', 0) AS userid,
      l.date_mod
    FROM glpi_tickets t
    LEFT JOIN glpi.glpi_logs l ON l.items_id = t.id AND l.itemtype = 'Ticket' AND l.itemtype_link = 'User'
    WHERE l.user_name != 'cron_mailgate' AND l.old_value=''
  ),
  records2 AS (
    SELECT
        records1.*, ROW_NUMBER() OVER(PARTITION BY records1.id ORDER BY records1.date_mod ASC) AS num
    FROM records1
    JOIN glpi.glpi_tickets_users tu ON records1.id = tu.tickets_id AND records1.userid = tu.users_id
    AND tu.type = 2
  )
  SELECT
    DISTINCT u.id AS EngineerNK, 
    u.name AS EngineerLogin, 
    u.firstname AS EngineerName, 
    u.realname AS EngineerSurname, 
    l.completename AS Location
  FROM records2 
  JOIN glpi.glpi_users u ON records2.userid = u.id
  INNER JOIN glpi.glpi_locations l ON u.locations_id = l.id  
  WHERE records2.num = 1
""")

# COMMAND ----------

ENGINEER_DF = spark.sql("""
  SELECT 
    DISTINCT u.id AS EngineerNK, 
    u.name AS EngineerLogin, 
    u.firstname AS EngineerName, 
    u.realname AS EngineerSurname, 
    l.completename AS Location
  FROM glpi_users u
  INNER JOIN glpi.glpi_tickets_users tu ON u.id = tu.users_id AND tu.type = 2
  LEFT JOIN glpi.glpi_locations l ON u.locations_id = l.id
""")

# COMMAND ----------

main_df = (
  ENGINEER_DF
  .union(FIRST_ENGINEER_DF)
  .drop_duplicates(subset=['EngineerNK'])
)

# COMMAND ----------

main_f = (
  main_df
    .transform(lower_column("EngineerLogin"))
    .transform(explode_location)
    .transform(rename_region)
    .transform(rename_country)
    .transform(rename_city)
    .transform(replace_nulls_with_unknown(['EngineerLogin', 'EngineerName', 'EngineerSurname', 'Region', 'Country', 'City']))
    .transform(replace_empty_with_unknown(['EngineerLogin', 'EngineerName', 'EngineerSurname', 'Region', 'Country', 'City']))
    .withColumnRenamed('Region', 'EngineerRegion')
    .withColumnRenamed('Country', 'EngineerCountry')
    .withColumnRenamed('City', 'EngineerCity')
    .transform(attach_source_column('GLPI'))
    .transform(attach_deleted_flag())
    .transform(attach_modified_date())
    .transform(attach_surrogate_key(['EngineerNK', '_SOURCE'], 'EngineerID'))
    .transform(attach_unknown_record(options = {'key_columns': ['EngineerID']}))
)

main_f.display()

# COMMAND ----------

# VALIDATE
check_distinct_count(main_f, ['EngineerID'])

# COMMAND ----------

 # LOAD
f_table_name = f'{database_name}.{table_name}'
register_hive_table(main_f, f_table_name, target_folder, options = {'overwrite': overwrite})
merge_into_table(main_f, f_table_name, 'EngineerID', options = {'auto_merge': True})  

# COMMAND ----------

# HANDLE DELETE
full_keys_f = (
  spark.table('glpi.glpi_users')
  .filter('_DELETED IS FALSE')
  .select('id')
  .transform(attach_source_column('GLPI'))
  .transform(attach_surrogate_key(['id', '_SOURCE'], 'EngineerID'))
  .transform(attach_unknown_record(options = {'key_columns': ['EngineerID']}))
  .select('EngineerID')
)

apply_soft_delete(full_keys_f, f_table_name, key_columns = 'EngineerID')

# COMMAND ----------


