# Databricks notebook source
# MAGIC %run ../_SHARED/FUNCTIONS/1.2/bootstrap

# COMMAND ----------

# MAGIC %run ../_SHARED/PARTIALS/1.2/header_s_core

# COMMAND ----------

# MAGIC %run ../_SCHEMA/s_core.territory

# COMMAND ----------

columns = list(schema.keys())

# COMMAND ----------

# LOAD DATASETS
if incremental:
  cutoff_value = get_cutoff_value(table_name, 'ebs.fnd_territories', prune_days)
  main_inc = load_incr_dataset('ebs.fnd_territories', 'LAST_UPDATE_DATE', cutoff_value)
else:
  main_inc = load_full_dataset('ebs.fnd_territories')

# COMMAND ----------

# SAMPLING
if sampling:
  main_inc = main_inc.limit(10)

# COMMAND ----------

# VIEWS
main_inc.createOrReplaceTempView('fnd_territories')

# COMMAND ----------

main = spark.sql("""Select
                    REPLACE(STRING(INT(FND_TERRITORIES.CREATED_BY)), ",", "")                        AS createdBy,
                    FND_TERRITORIES.CREATION_DATE                                                    AS createdOn,
                    REPLACE(STRING(INT (FND_TERRITORIES.LAST_UPDATED_BY)), ",", "")                  AS modifiedBy,
                    FND_TERRITORIES.LAST_UPDATE_DATE                                                 AS modifiedOn,
                    CURRENT_TIMESTAMP()                                                              AS insertedOn,
                    CURRENT_TIMESTAMP()                                                              AS updatedOn,
                    FND_TERRITORIES_TL.TERRITORY_CODE                                                AS territoryCode,
                    FND_TERRITORIES_TL.TERRITORY_SHORT_NAME                                          AS territoryShortName,
                    FND_TERRITORIES_TL.DESCRIPTION                                                   AS territoryDescription,
                    FND_TERRITORIES.EU_CODE                                                          AS euCode,
                    spt.subRegion                                                                    AS subRegion,
                    spt.CorporatesubRegion                                                           AS subRegionGis,
                    spt.ManagementArea                                                               AS managementArea,
                    spt.Region                                                                       AS region,
                    spt.ForecastArea                                                                 AS forecastArea,
                    FND_TERRITORIES.OBSOLETE_FLAG                                                    AS obsoleteFlag,
                    FND_TERRITORIES.ISO_NUMERIC_CODE                                                 AS isoCodeNumeric,
                    FND_TERRITORIES.ISO_TERRITORY_CODE                                               AS isoCode,
                    FND_TERRITORIES.NLS_TERRITORY                                                    AS nlsCode
                    from fnd_territories 
                    Left Join ebs.fnd_territories_tl  ON fnd_territories.territory_code=fnd_territories_tl.territory_code
                    Left Join spt.E2EcountriesMasterTable spt ON  fnd_territories.territory_code = spt.country 
                    WHERE FND_TERRITORIES_TL.LANGUAGE='US'""")
main.cache()
display(main)

# COMMAND ----------

main = remove_duplicate_rows(df = main, 
                             key_columns = ['territoryCode'], 
                             tableName = table_name, 
                             sourceName = source_name, 
                             notebookName = NOTEBOOK_NAME, 
                             notebookPath = NOTEBOOK_PATH)

# COMMAND ----------

# TRANSFORM DATA
main_f = (
  main
  .transform(tg_default(source_name))
  .transform(tg_core_territory())
  .transform(apply_schema(schema))
  .transform(attach_unknown_record)
  .select(columns)
  .transform(sort_columns)
)

main_f.cache()
main_f.display()

# COMMAND ----------

# PERSIST DATA
options = {'target_storage': target_storage, 'target_container': target_container}
merge_to_delta(main_f, table_name, target_folder, overwrite, options = options)

# COMMAND ----------

# HANDLE DELETE
full_keys_f = (
  spark.sql("""
            SELECT 
                FND_TERRITORIES_TL.TERRITORY_CODE AS territoryCode
                from ebs.fnd_territories 
                Left Join ebs.fnd_territories_tl  ON fnd_territories.territory_code=fnd_territories_tl.territory_code
                WHERE FND_TERRITORIES_TL.LANGUAGE='US'
            """)
  .transform(attach_source_column(source = source_name))
  .transform(attach_surrogate_key(columns = 'territoryCode,_SOURCE'))
  .select('_ID')
  .transform(add_unknown_ID())
)

apply_soft_delete(full_keys_f, table_name, key_columns = '_ID')

# COMMAND ----------

# UPDATE FK
update_foreign_key(table_name, 'createdBy,_SOURCE', 'createdBy_ID', 'edm.user')
update_foreign_key(table_name, 'modifiedBy,_SOURCE', 'modifiedBy_ID', 'edm.user')

# COMMAND ----------

# UPDATE CUTOFF VALUE
if not test_run:
  cutoff_value = get_incr_col_max_value(main_inc, 'LAST_UPDATE_DATE')
  update_cutoff_value(cutoff_value, table_name, 'ebs.fnd_territories')
  update_run_datetime(run_datetime, table_name, 'ebs.fnd_territories')

# COMMAND ----------

# MAGIC %run ../_SHARED/PARTIALS/1.2/footer_s_core
