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
main_inc = load_full_dataset('kgd.dbo_tembo_customer')

# COMMAND ----------

# SAMPLING
if sampling:
  main_inc = main_inc.limit(10)

# COMMAND ----------

# VIEWS
main_inc.createOrReplaceTempView('fnd_territories')

# COMMAND ----------

main = spark.sql("""Select distinct 
                    NULL AS createdBy,
                    NULL AS createdOn,
                    NULL AS modifiedBy,
                    NULL AS modifiedOn,
                    CURRENT_TIMESTAMP() AS insertedOn,
                    CURRENT_TIMESTAMP() AS updatedOn,
                    case when addresscountry = 'China' then 'CN'
                      else addresscountry
                    end AS territoryCode,
                    addresscountry AS territoryShortName,
                    addresscountry AS territoryDescription,
                    case when addresscountry = 'China' then 'CN'
                      else addresscountry
                    end AS euCode,
                    spt.subRegion AS subRegion,
                    spt.CorporatesubRegion AS subRegionGis,
                    spt.ManagementArea AS managementArea,
                    spt.Region AS region,
                    spt.ForecastArea AS forecastArea,
                    False AS obsoleteFlag,
                    '156' AS isoCodeNumeric,
                    'CHN' AS isoCode,
                    'CHINA' AS nlsCode
                    from fnd_territories 
                         Left Join spt.E2EcountriesMasterTable spt ON spt.country = case when addresscountry = 'China' then 'CN' else addresscountry end
                     where NOT(fnd_territories._DELETED)
                     and addresscountry = 'China'
                    """)
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
                case when addresscountry = 'China' then 'CN'
                      else addresscountry
                    end AS territoryCode
                from kgd.dbo_tembo_customer
                where NOT(_DELETED)
                and addresscountry = 'China' 
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
# if not test_run:
#   cutoff_value = get_incr_col_max_value(main_inc, 'LAST_UPDATE_DATE')
#   update_cutoff_value(cutoff_value, table_name, 'ebs.fnd_territories')
#   update_run_datetime(run_datetime, table_name, 'ebs.fnd_territories')

# COMMAND ----------

# MAGIC %run ../_SHARED/PARTIALS/1.2/footer_s_core
