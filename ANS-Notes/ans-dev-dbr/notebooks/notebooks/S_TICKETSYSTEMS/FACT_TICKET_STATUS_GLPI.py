# Databricks notebook source
# MAGIC %run ../_SHARED/FUNCTIONS/1.3/bootstrap

# COMMAND ----------

# MAGIC %run ./_SHARED/glpi_functions

# COMMAND ----------

# INPUT PARAMETERS
database_name = get_input_param('database_name', default_value = 's_ticketsystems')
overwrite = get_input_param('overwrite', 'bool', default_value = True)
sampling = get_input_param('sampling', 'bool', default_value = False)
table_name = get_input_param('table_name', default_value = 'fact_ticket_status_glpi')
target_folder = get_input_param('target_folder', default_value = '/datalake_silver/ticketsystems/full_data')

# COMMAND ----------

# EXTRACT
glpi_tickets = spark.table('glpi.glpi_tickets')
glpi_tickets.createOrReplaceTempView("glpi_tickets")

# COMMAND ----------

TICKET_STATUS_DF = spark.sql("""
  SELECT 
    DISTINCT t.id AS TicketNK, 
    IF(t.entities_id = 0, NULL, t.entities_id) AS EntityNK,
    IF(t.itilcategories_id = 0, NULL, t.itilcategories_id) AS TicketCategoryNK,
    IF(ctu.users_id = 0, NULL, ctu.users_id) AS CustomerNK,
    IF(gt.groups_id = 0, NULL, gt.groups_id) AS SupportGroupNK,
    IF(t.date_creation IS NOT NULL AND t.date_creation < t.date, t.date_creation, t.date) AS CreatedDate,
    t.date_mod AS ModifiedDate, 
    t.time_to_resolve AS DueDate,
    t.time_to_own AS TimeToOwnDate,
    t.solvedate AS SolveDate, 
    t.closedate AS CloseDate, 
    t.actiontime AS ActionTime,
    ts.satisfaction AS Satisfaction,
    t.sla_waiting_duration AS WaitingDurationSla,
    t.waiting_duration AS WaitingDuration,
    NVL(CAST(t.is_deleted AS BOOLEAN), False) AS IsDeleted,
    IF(l.user_name IS NOT NULL, 1, 0) AS mailgate,
    IF(l2.new_value IS NOT NULL, 1, 0) AS sla_change
  FROM glpi_tickets t
  LEFT JOIN glpi.glpi_tickets_users ctu ON t.id = ctu.tickets_id AND ctu.type = 1
  LEFT JOIN glpi.glpi_groups_tickets gt ON t.id = gt.tickets_id AND gt.type = 2
  LEFT JOIN glpi.glpi_ticketsatisfactions ts ON t.id = ts.tickets_id
  LEFT JOIN glpi.glpi_logs l ON t.id = l.items_id AND l.itemtype = 'Ticket' AND l.user_name = 'cron_mailgate'
  LEFT JOIN glpi.glpi_logs l2 ON 
    t.id = l2.items_id 
    AND l2.itemtype = 'Ticket' 
    AND l2.id_search_option = 30 
    AND l2.new_value IN ('SLA - 1 Day (2)', 'SLA - 3 Days (3)', 'SLA - 5 Days (4)')
  WHERE t.date_creation >= '2018-01-01' 
""")

TICKET_STATUS_DF.createOrReplaceTempView("TICKET_STATUS_DF")
TICKET_STATUS_DF.display()

# COMMAND ----------

FA_ENGINEERS_DF = spark.sql("""
  WITH records1 AS (
  SELECT
      t.id,
      REGEXP_EXTRACT(l.new_value, '(\\\d+)', 0) AS userid,
      l.date_mod
    FROM glpi_tickets t
    JOIN glpi.glpi_logs l ON l.items_id = t.id AND l.itemtype = 'Ticket' AND l.itemtype_link = 'User'
    WHERE l.old_value=''
  ), records2 AS (
    SELECT
      records1.*, ROW_NUMBER() OVER(PARTITION BY records1.id ORDER BY records1.date_mod ASC) AS num
    FROM records1
    JOIN glpi.glpi_tickets_users tu ON records1.id = tu.tickets_id AND records1.userid = tu.users_id AND tu.type = 2
  )
  SELECT 
    id AS TicketNK,
    num,
    userid AS FirstEngineerNK,
    date_mod AS AssignmentDate
  FROM records2
  WHERE num = 1
""")

FA_ENGINEERS_DF.createOrReplaceTempView("FA_ENGINEERS_DF")

# COMMAND ----------

main_df = spark.sql("""
  SELECT 
    t.TicketNK, 
    t.EntityNK, 
    t.TicketCategoryNK, 
    t.CustomerNK,
    IF(e.FirstEngineerNK = 0, NULL, e.FirstEngineerNK) AS FirstEngineerNK,
    t.SupportGroupNK,
    t.CreatedDate, 
    e.AssignmentDate, 
    t.ModifiedDate, 
    t.DueDate, 
    t.TimeToOwnDate, 
    t.SolveDate, 
    t.CloseDate, 
    t.ActionTime,
    t.Satisfaction,
    t.WaitingDurationSla,
    t.WaitingDuration,
    t.IsDeleted,
    t.mailgate,
    t.sla_change
  FROM TICKET_STATUS_DF t
  LEFT JOIN FA_ENGINEERS_DF e ON t.TicketNK = e.TicketNK
""")

# COMMAND ----------

main_df2 = (
  main_df
    .transform(drop_duplicates(['TicketNK']))
    .transform(localize_cst6cdt).drop('mailgate').drop('sla_change')
    .transform(attach_source_column('GLPI'))
    .transform(attach_deleted_flag())
    .transform(attach_modified_date())
    .transform(attach_surrogate_key(['TicketNK', '_SOURCE'], 'TicketID'))
    .transform(attach_surrogate_key(['EntityNK', '_SOURCE'], 'EntityID'))
    .transform(attach_surrogate_key(['TicketCategoryNK', '_SOURCE'], 'TicketCategoryID'))
    .transform(attach_surrogate_key(['CustomerNK', '_SOURCE'], 'CustomerID'))
    .transform(attach_surrogate_key(['FirstEngineerNK', '_SOURCE'], 'FirstEngineerID'))
    .transform(attach_surrogate_key(['SupportGroupNK', '_SOURCE'], 'SupportGroupID'))    
)

main_df2.createOrReplaceTempView('main_df2')

# COMMAND ----------

main_f = spark.sql("""
  SELECT 
    TicketID,
    EntityID,
    TicketCategoryID,
    CustomerID,
    FirstEngineerID,
    SupportGroupID,    
    ActionTime,
    Satisfaction,
    WaitingDurationSla,
    WaitingDuration,
    IsDeleted,      
    CAST(CAST(CreatedDate AS TIMESTAMP) AS DATE) AS CreatedDate, 
    CAST(CAST(AssignmentDate AS TIMESTAMP) AS DATE) AS AssignmentDate, 
    CAST(CAST(ModifiedDate AS TIMESTAMP) AS DATE) AS ModifiedDate, 
    CAST(CAST(DueDate AS TIMESTAMP) AS DATE) AS DueDate, 
    CAST(CAST(TimeToOwnDate AS TIMESTAMP) AS DATE) AS TimeToOwnDate, 
    CAST(CAST(SolveDate AS TIMESTAMP) AS DATE) AS SolveDate, 
    CAST(CAST(CloseDate AS TIMESTAMP) AS DATE) AS CloseDate,
    _SOURCE,
    _DELETED,
    _MODIFIED
  FROM main_df2
""")

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
  .transform(attach_source_column('GLPI'))
  .transform(attach_surrogate_key(['id', '_SOURCE'], 'TicketID'))
  .select('TicketID')
)

apply_soft_delete(full_keys_f, f_table_name, key_columns = 'TicketID')

# COMMAND ----------


