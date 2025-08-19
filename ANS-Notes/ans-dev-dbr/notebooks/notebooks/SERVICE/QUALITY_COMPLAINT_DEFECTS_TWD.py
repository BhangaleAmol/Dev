# Databricks notebook source
# MAGIC %run ../_SHARED/FUNCTIONS/1.2/bootstrap

# COMMAND ----------

# MAGIC %run ../_SHARED/PARTIALS/1.2/header_s_service

# COMMAND ----------

# MAGIC %run ../_SCHEMA/s_service.quality_complaint_defects

# COMMAND ----------

# LOAD DATASETS
if incremental:
  cutoff_value = get_cutoff_value(table_name, 'twd.complainttype', prune_days)
  complainttype = load_incr_dataset('twd.complainttype', 'SystemModstamp', cutoff_value)
else:
  complainttype = load_full_dataset('twd.complainttype')

masterlistbydefect = load_full_dataset('twd.masterlistbydefect')

# COMMAND ----------

# SAMPLING
if sampling:
  complainttype = complainttype.limit(10)
  masterlistbydefect = masterlistbydefect.limit(10)

# COMMAND ----------

# VIEWS
complainttype.createOrReplaceTempView('complainttype')
masterlistbydefect.createOrReplaceTempView('masterlistbydefect')

# COMMAND ----------

complainttype_2 = spark.sql("""
SELECT   
	c.CreatedById AS createdBy,
	c.CreatedDate AS createdOn,
	c.LastModifiedById AS modifiedBy,
	c.LastModifiedDate AS modifiedOn,
	CURRENT_TIMESTAMP AS insertedOn,
	CURRENT_TIMESTAMP AS updatedOn, 
	c.ID AS complaintTypeId,
	c.Name AS complaintTypeName,
	c.Complaint__c AS complaintId,
	c.Additional_Information__c AS additionalInfo,
	c.Complaint_Type__c AS complaintType,
	c.Complaint_Sub_Type__c AS complaintSubType,
	c.IsDeleted AS isDeleted,
	NULL AS productBrand,
	NULL AS productStyle
FROM complainttype c
""")

complainttype_f = complainttype_2.transform(apply_schema(schema))
complainttype_f.cache()
complainttype_f.display()

# COMMAND ----------

masterlistbydefect_2 = spark.sql("""
SELECT 
	'System' AS createdBy,
	TO_TIMESTAMP(DATE, 'yyyy-MM-dd') AS createdOn,
	'System' AS  modifiedBy,
	TO_TIMESTAMP(DATE, 'yyyy-MM-dd') AS modifiedOn,
	CURRENT_TIMESTAMP AS insertedOn,
	CURRENT_TIMESTAMP AS updatedOn, 
	RecordID 
		|| "-" || DATE_FORMAT(DATE, 'yyyyMMdd')
		|| "-" || COALESCE(ComplaintType, '') 
		|| "-" || COALESCE(Subdivision , '') 
		|| "-" || COALESCE(Country, '') 
		|| "-" || COALESCE(Region, '') 
		|| "-" || COALESCE(BatchLot, '') 
		|| "-" || COALESCE(OriginatingMfg, '') 
		|| "-" || COALESCE(ProductBrand, '') 
		|| "-" || COALESCE(ProductStyle, '') 
		|| "-" || COALESCE(ProductCategory , '') 
		|| "-" || COALESCE(Defect, '') AS complaintTypeId,
	defect AS complaintTypeName,
	recordID AS complaintid,
	NULL AS complaintAdditionalInfo,
	defect AS complaintType,
	NULL AS complaintSubType,
	FALSE AS isDeleted,
	ProductBrandCorrected AS productBrand,
	ProductStyle AS productStyle
FROM masterlistbydefect
""")

masterlistbydefect_f = masterlistbydefect_2.transform(apply_schema(schema))
masterlistbydefect_f.cache()
masterlistbydefect_f.display()

# COMMAND ----------

main = complainttype_f.union(masterlistbydefect_f)
main.display()

# COMMAND ----------

columns = list(schema.keys())

# COMMAND ----------

# TRANSFORM DATA
main_f = (
  main
  .transform(tg_default(source_name))
  .transform(tg_service_quality_complaint_defects())
  .transform(apply_schema(schema))  
  .transform(attach_unknown_record)
  .select(columns)
  .transform(sort_columns)
)

main_f.cache()
main_f.display()

# COMMAND ----------

# VALIDATE DATA
valid_count_rows(main_f, '_ID')

# COMMAND ----------

# CHECK DATA QUALITY
show_null_values(main_f)

# COMMAND ----------

# PERSIST DATA
options = {'target_storage': target_storage, 'target_container': target_container}
merge_to_delta(main_f, table_name, target_folder, overwrite, options = options)

# COMMAND ----------

# HANDLE DELETE

full_ct_keys = (
  spark.table('twd.complainttype')
  .filter('_DELETED IS FALSE')
  .selectExpr("ID AS complaintTypeId")
)

full_mld_keys = spark.sql("""
  SELECT
    RecordID 
    || "-" || DATE_FORMAT(DATE, 'yyyyMMdd')
    || "-" || COALESCE(ComplaintType, '') 
    || "-" || COALESCE(Subdivision , '') 
    || "-" || COALESCE(Country, '') 
    || "-" || COALESCE(Region, '') 
    || "-" || COALESCE(BatchLot, '') 
    || "-" || COALESCE(OriginatingMfg, '') 
    || "-" || COALESCE(ProductBrand, '') 
    || "-" || COALESCE(ProductStyle, '') 
    || "-" || COALESCE(ProductCategory , '') 
    || "-" || COALESCE(Defect, '') AS complaintTypeId
  FROM twd.masterlistbydefect
  WHERE _DELETED IS FALSE
""")

full_keys_f = (
  full_ct_keys.union(full_mld_keys)
  .transform(attach_source_column(source = source_name))
  .transform(attach_surrogate_key(columns = 'complaintTypeId,_SOURCE'))
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

  cutoff_value = get_incr_col_max_value(complainttype, "SystemModstamp")
  update_cutoff_value(cutoff_value, table_name, 'twd.complainttype')
  update_run_datetime(run_datetime, table_name, 'twd.complainttype')

  cutoff_value = get_incr_col_max_value(masterlistbydefect)
  update_cutoff_value(cutoff_value, table_name, 'twd.masterlistbydefect')
  update_run_datetime(run_datetime, table_name, 'twd.masterlistbydefect')

# COMMAND ----------

# MAGIC %run ../_SHARED/PARTIALS/1.2/footer_s_service
