# Databricks notebook source
# MAGIC %run ../_SHARED/FUNCTIONS/1.2/bootstrap

# COMMAND ----------

# MAGIC %run ../_SHARED/PARTIALS/1.2/header_s_core

# COMMAND ----------

# MAGIC %run ../_SCHEMA/s_core.substitution_master

# COMMAND ----------

# LOAD DATASETS
if incremental:
  cutoff_value = get_cutoff_value(table_name, 'spt.globalproductsubstitutionmaster', prune_days)
  substitution_master = load_incr_dataset('spt.globalproductsubstitutionmaster', '_MODIFIED', cutoff_value)
else:
  substitution_master = load_full_dataset('spt.globalproductsubstitutionmaster')

# COMMAND ----------

# SAMPLING
if sampling:
  substitution_master = substitution_master.limit(10)

# COMMAND ----------

# VIEWS
substitution_master.createOrReplaceTempView('substitution_master')

# COMMAND ----------

main = spark.sql("""
select 
    substituteMaster.CreatedById AS createdBy,
    substituteMaster.Created AS createdOn,
    substituteMaster.ModifiedById AS modifiedBy,
    substituteMaster.Modified AS modifiedOn,
    CURRENT_TIMESTAMP() AS insertedOn,
    CURRENT_TIMESTAMP() AS updatedOn,
    substituteMaster.ComplianceAssetId AS ComplianceAssetId,
    substituteMaster.ContentType AS ContentType,
    substituteMaster.ContentTypeID AS ContentTypeID,
    substituteMaster.DCValue AS DCValue,
    substituteMaster.DSWHValue AS DSWHValue,
    substituteMaster.Owshiddenversion AS Owshiddenversion,
    substituteMaster.Path AS Path,
    substituteMaster.PhaseInDate AS PhaseInDate,
    TO_timestamp(substituteMaster.phaseoutdate, "MM/dd/yyyy HH:mm:ss") AS PhaseOutDate,
    substituteMaster.PredecessorCode AS PredecessorCode,
    substituteMaster.PredecessorCodeDescription AS PredecessorCodeDescription,
    substituteMaster.SubRegionValue AS SubRegionValue,
    substituteMaster.Id AS substitutionId,
    substituteMaster.successorcode AS SuccessorCode,
    substituteMaster.Version AS Version
from substitution_master substituteMaster
WHERE not(_DELETED)
""")

main.cache()
display(main)

# COMMAND ----------

main = remove_duplicate_rows(df = main, 
                             key_columns = ['substitutionId'], 
                             tableName = table_name, 
                             sourceName = source_name, 
                             notebookName = NOTEBOOK_NAME, 
                             notebookPath = NOTEBOOK_PATH)

# COMMAND ----------

# TRANSFORM DATA
main_f = (
  main
  .transform(tg_default(source_name))
  .transform(tg_core_substitution_master())
  .transform(apply_schema(schema))
)

main_f.cache()
main_f.display()

# COMMAND ----------

# PERSIST DATA
options = {'target_storage': target_storage, 'target_container': target_container}
merge_to_delta(main_f, table_name, target_folder, overwrite, options = options)
add_unknown_record(main_f, table_name)

# COMMAND ----------

# HANDLE DELETE
full_keys = spark.sql("""
SELECT DISTINCT 
    substituteMaster.Id AS substitutionId
from spt.globalproductsubstitutionmaster substituteMaster
where not(_deleted)
""") 

full_keys_f = (
  full_keys
  .transform(attach_source_column(source = source_name))
  .transform(attach_surrogate_key('substitutionId,_SOURCE'))
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
  cutoff_value = get_incr_col_max_value(substitution_master,'_MODIFIED')
  update_cutoff_value(cutoff_value, table_name, 'spt.globalproductsubstitutionmaster')
  update_run_datetime(run_datetime, table_name, 'spt.globalproductsubstitutionmaster')

# COMMAND ----------

# MAGIC %run ../_SHARED/PARTIALS/1.2/footer_s_core
