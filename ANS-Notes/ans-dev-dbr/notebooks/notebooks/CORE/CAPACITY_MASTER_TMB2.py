# Databricks notebook source
# MAGIC %run ../_SHARED/FUNCTIONS/1.2/bootstrap

# COMMAND ----------

# MAGIC %run ../_SHARED/PARTIALS/1.2/header_s_core

# COMMAND ----------

# MAGIC %run ../_SCHEMA/s_core.capacity_master

# COMMAND ----------

overwrite = True

# COMMAND ----------

# LOAD DATASETS
dbo_cm_raw_data = load_full_dataset('tmb2.dbo_cm_raw_data')
dbo_cm_raw_data.createOrReplaceTempView('dbo_cm_raw_data')

# COMMAND ----------

# SAMPLING
if sampling:
  dbo_cm_raw_data = dbo_cm_raw_data.limit(10)
  dbo_cm_raw_data.createOrReplaceTempView('dbo_cm_raw_data')

# COMMAND ----------

def t_get_primary_records(df):
  df.createOrReplaceTempView('df')  
  return spark.sql("""
    SELECT
      raw.createdBy,
      raw.createdDate,
      raw.modifiedBy,
      raw.modifiedDate,
      res.ResourceIndex, 
      res.SecondaryResLink, 
      res.SecSecondaryResLink, 
      raw.Line, 
      raw.CapacityGroup, 
      raw.Size, 
      raw.Location, 
      raw.Style, 
      raw.OEEPct, 
      raw.UOM,
      res.ResourceType, 
      res.RateOrUnit, 
      res.GrossDailyOutput, 
      res.HoursPerDay, 
      CAST(res.EffectiveDate AS DATE) AS EffectiveDate,
      res.OvertimeCapacity,
      res.OvertimeCost,
      res.PlannerID,
      res.PlanningTimeFence,
      res.ResourcePriority,
      res.ConstrainedResIndi,
      res.ConstrActiveIndi,
      res.MIN_USG_HRS_QTY,
      res.Calendar,
      res.BatchID,
      res.ResLevelLoadIndicator      
    FROM df raw
    LEFT JOIN tmb2.dbo_cm_resource_data res ON 
      raw.Line = res.Line 
      AND raw.CapacityGroup = res.CapacityGroup 
      AND raw.Size = res.Size 
      AND raw.Location = res.Location
      AND raw.UOM = res.UOM
    WHERE res.ResourceType = 'Primary';
  """)

# COMMAND ----------

def t_get_secondary_records(df):
  df.createOrReplaceTempView('df')  
  return spark.sql("""
    SELECT
      m.createdBy,
      m.createdDate,
      m.modifiedBy,
      m.modifiedDate,
      res.ResourceIndex, 
      res.SecondaryResLink, 
      res.SecSecondaryResLink, 
      m.Line, 
      m.CapacityGroup, 
      m.Size, 
      m.Location, 
      m.Style, 
      m.OEEPct, 
      m.UOM,
      res.ResourceType, 
      res.RateOrUnit, 
      IFNULL(res.GrossDailyOutput, 0) AS GrossDailyOutput,
      res.HoursPerDay,
      CAST(res.EffectiveDate AS DATE) AS EffectiveDate,
      res.OvertimeCapacity,
      res.OvertimeCost,
      res.PlannerID,
      res.PlanningTimeFence,
      res.ResourcePriority,
      res.ConstrainedResIndi,
      res.ConstrActiveIndi,
      res.MIN_USG_HRS_QTY,
      res.Calendar,
      res.BatchID,
      res.ResLevelLoadIndicator      
    FROM df m
    JOIN tmb2.dbo_cm_resource_data res ON 
      res.ResourceIndex = m.SecondaryResLink
      AND res.UOM = m.UOM
    WHERE 
      m.SecondaryResLink IS NOT NULL 
      AND m.SecondaryResLink != ''
      AND res.ResourceType = 'Secondary'
      -- generate secondary for actual primary only
      AND CURRENT_DATE() BETWEEN m.BeginDate AND m.EndDate
  """)

# COMMAND ----------

def t_get_sec_secondary_records(df):  
  df.createOrReplaceTempView('df')  
  return spark.sql("""
    SELECT
      m.createdBy,
      m.createdDate,
      m.modifiedBy,
      m.modifiedDate,
      res.ResourceIndex, 
      res.SecondaryResLink, 
      res.SecSecondaryResLink, 
      m.Line, 
      m.CapacityGroup, 
      m.Size, 
      m.Location, 
      m.Style, 
      m.OEEPct, 
      m.UOM,
      res.ResourceType, 
      res.RateOrUnit, 
      IFNULL(res.GrossDailyOutput, 0) AS GrossDailyOutput,
      res.HoursPerDay,
      CAST(res.EffectiveDate AS DATE) AS EffectiveDate,
      res.OvertimeCapacity,
      res.OvertimeCost,
      res.PlannerID,
      res.PlanningTimeFence,
      res.ResourcePriority,
      res.ConstrainedResIndi,
      res.ConstrActiveIndi,
      res.MIN_USG_HRS_QTY,
      res.Calendar,
      res.BatchID,
      res.ResLevelLoadIndicator      
    FROM df m
    JOIN tmb2.dbo_cm_resource_data res ON 
      res.ResourceIndex = m.SecSecondaryResLink
      AND res.UOM = m.UOM
    WHERE 
      m.SecSecondaryResLink IS NOT NULL 
      AND m.SecSecondaryResLink != ''
      AND res.ResourceType = 'Sec Secondary'
      -- generate sec secondary for actual primary only   
      AND CURRENT_DATE() BETWEEN m.BeginDate AND m.EndDate  
  """)

# COMMAND ----------

def t_add_begin_and_end_dates(df): 
  df.createOrReplaceTempView('df')  
  return spark.sql("""
    WITH df_rownum AS (
      SELECT 
        *,
        ROW_NUMBER() 
          OVER (
          PARTITION BY Line, CapacityGroup, Size, Location, ResourceType, Style, UOM
          ORDER BY EffectiveDate DESC) 
        AS RowNum
      FROM df
    )
    -- the latest effective date
    SELECT a.*, 
      a.EffectiveDate AS BeginDate,
      CAST('2999-12-12' AS DATE) AS EndDate 
    FROM df_rownum a
    WHERE a.RowNum = 1
    UNION  
    -- previous effective dates
    SELECT a.*, 
      a.EffectiveDate AS BeginDate,
      DATE_SUB(b.EffectiveDate, 1) AS EndDate 
    FROM df_rownum a
    LEFT JOIN df_rownum b ON 
      a.Line = b.Line 
      AND a.CapacityGroup = b.CapacityGroup
      AND a.Size = b.Size
      AND a.Location = b.Location
      AND a.Style = b.Style
    WHERE b.RowNum = a.RowNum - 1; 
  """)

# COMMAND ----------

def t_get_main_dataset(df):
  
  primary_f = (
    df
    .transform(t_get_primary_records)
    .transform(t_add_begin_and_end_dates)
  )
  
  secondary_f = (
    primary_f
    .transform(t_get_secondary_records)
    .transform(t_add_begin_and_end_dates)
  )
  
  sec_secondary_f = (
    primary_f
    .transform(t_get_sec_secondary_records)
    .transform(t_add_begin_and_end_dates)
  )

  return primary_f.union(secondary_f).union(sec_secondary_f)

# COMMAND ----------

def t_add_regular_capacity_quantity(df): 
  df.createOrReplaceTempView('df')  
  return spark.sql("""
    SELECT 
      m.*,  
      CASE 
        WHEN m.RateOrUnit = 'U' THEN m.GrossDailyOutput
        ELSE m.HoursPerDay
      END AS regularCapacityQuantity
  FROM df m
  """)

# COMMAND ----------

def t_add_units_per_hour(df): 
  df.createOrReplaceTempView('df')  
  return spark.sql("""
    WITH df2 AS (
      SELECT 
        m.*,
        CASE
          WHEN m.ResourceType = 'Primary' AND m.RateOrUnit = 'U' 
          THEN 1 * m.OEEPct
          WHEN m.ResourceType = 'Primary' AND m.RateOrUnit <> 'U' 
          THEN m.GrossDailyOutput * m.OEEPct / m.HoursPerDay
          ELSE NULL
        END AS UnitsPerHourTmp
      FROM df m
    )
    SELECT 
      m.*,
      CASE
        WHEN m.ResourceType = 'Primary' 
        THEN m.UnitsPerHourTmp
        WHEN m.ResourceType = 'Secondary' AND m.RateOrUnit = 'U' 
        THEN 1 * m.OEEPct
        WHEN m.ResourceType = 'Secondary' AND m.GrossDailyOutput > 0 AND m.RateOrUnit = 'R' 
        THEN m.GrossDailyOutput * m.OEEPct / m.HoursPerDay
        WHEN m.ResourceType = 'Secondary' AND m.GrossDailyOutput = 0 AND m.RateOrUnit = 'R' 
        THEN p4s.UnitsPerHourTmp * p4s.regularCapacityQuantity / m.regularCapacityQuantity        
        WHEN m.ResourceType = 'Sec Secondary' AND m.RateOrUnit = 'U' 
        THEN 1 * m.OEEPct
        WHEN m.ResourceType = 'Sec Secondary' AND m.GrossDailyOutput > 0 AND m.RateOrUnit = 'R' 
        THEN m.GrossDailyOutput * m.OEEPct / m.HoursPerDay
        WHEN m.ResourceType = 'Sec Secondary' AND m.GrossDailyOutput = 0 AND m.RateOrUnit = 'R' 
        THEN p4ss.UnitsPerHourTmp * p4ss.regularCapacityQuantity / m.regularCapacityQuantity        
      END AS UnitsPerHour
    FROM df2 m
    
    -- primary data for secondary
    LEFT JOIN df2 p4s ON 
      m.ResourceIndex = p4s.SecondaryResLink
      AND m.Line = p4s.Line
      AND m.CapacityGroup = p4s.CapacityGroup
      AND m.Size = p4s.Size
      AND m.Style = p4s.Style
      AND m.Location = p4s.Location
      AND CURRENT_DATE() BETWEEN p4s.BeginDate AND p4s.EndDate -- join actual primary only
      
    -- primary data for sec secondary
    LEFT JOIN df2 p4ss ON 
      m.ResourceIndex = p4ss.SecSecondaryResLink
      AND m.Line = p4ss.Line
      AND m.CapacityGroup = p4ss.CapacityGroup
      AND m.Size = p4ss.Size
      AND m.Style = p4ss.Style
      AND m.Location = p4ss.Location
      AND CURRENT_DATE() BETWEEN p4ss.BeginDate AND p4ss.EndDate      
  """)

# COMMAND ----------

def t_get_final_columns(df): 
  df.createOrReplaceTempView('df')  
  return spark.sql("""
    SELECT
      createdBy,
      createdDate AS createdOn,
      modifiedBy,
      modifiedDate AS modifiedOn,
      Line AS line,
      CapacityGroup AS capacityGroup,
      Style AS style,
      Size AS size,
      Location AS location,
      UOM AS uom,
      BeginDate AS beginDate,
      EndDate AS endDate,
      OvertimeCapacity,
      OvertimeCost,
      plannerId,
      PlanningTimeFence,
      ResourceIndex AS productionResource,
      ResourcePriority,
      ResourceType AS resourceType,
      RateOrUnit AS rateOrUnit,
      GrossDailyOutput AS grossOutput,
      regularCapacityQuantity,
      OEEPct AS oeePct,
      UnitsPerHour AS unitsPerHour,
      ConstrainedResIndi,
      ConstrActiveIndi,
      Min_Usg_Hrs_Qty as MinUsgHrsQty,
      Calendar,
      BatchID,
      ResLevelLoadIndicator  
    FROM df  
  """)

# COMMAND ----------

def t_get_key_columns(df):
  df.createOrReplaceTempView('df')  
  return spark.sql("""
    SELECT     
      createdDate AS createdOn,
      Line AS line,
      CapacityGroup AS capacityGroup,
      Style AS style,
      Size AS size,
      Location AS location,
      UOM AS uom,
      BeginDate AS beginDate,
      ResourceType
    FROM df  
  """)

# COMMAND ----------

main_1 = t_get_main_dataset(dbo_cm_raw_data)

# COMMAND ----------

nk_columns = ['Line', 'CapacityGroup', 'Size', 'Location', 'ResourceType', 'Style', 'UOM', 'EffectiveDate']
main_2 = remove_duplicate_rows(main_1, nk_columns, table_name, source_name, NOTEBOOK_NAME, NOTEBOOK_PATH)

# COMMAND ----------

main_3 = (
  main_2
  .transform(t_add_regular_capacity_quantity)
  .transform(t_add_units_per_hour)
  .transform(t_get_final_columns)
)

# COMMAND ----------

# TRANSFORM DATA
columns = list(schema.keys())

main_f = (
  main_3
  .transform(tg_default(source_name))
  .transform(tg_core_capacity_master)
  .transform(apply_schema(schema))
  .transform(attach_unknown_record)
  .select(columns)
  .transform(sort_columns)
)

# COMMAND ----------

# PERSIST DATA
options = {'target_storage': target_storage, 'target_container': target_container}
merge_to_delta(main_f, table_name, target_folder, overwrite, options = options)

# COMMAND ----------

# HANDLE DELETE
handle_delete = True
if handle_delete:

  full_keys_f = (
    spark.table('tmb2.dbo_cm_raw_data')
    .filter('_DELETED IS FALSE')
    .transform(t_get_main_dataset)
    .transform(t_get_key_columns) 
    .transform(tg_default(source_name))
    .transform(tg_core_capacity_master)
    .transform(apply_schema(schema))
    .transform(attach_unknown_record)
    .select('_ID') 
  )

  apply_soft_delete(full_keys_f, table_name, key_columns = '_ID')

# COMMAND ----------

# UPDATE CUTOFF VALUE
if not test_run:
  cutoff_value = get_incr_col_max_value(dbo_cm_raw_data)
  update_cutoff_value(cutoff_value, table_name, 'tmb2.dbo_cm_raw_data')
  update_run_datetime(run_datetime, table_name, 'tmb2.dbo_cm_raw_data')

# COMMAND ----------

# MAGIC %run ../_SHARED/PARTIALS/1.2/footer_s_core
