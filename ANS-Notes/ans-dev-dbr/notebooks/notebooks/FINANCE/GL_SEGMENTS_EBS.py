# Databricks notebook source
# MAGIC %run ../_SHARED/FUNCTIONS/1.2/bootstrap

# COMMAND ----------

# MAGIC %run ../_SHARED/PARTIALS/1.2/header_s_finance

# COMMAND ----------

# MAGIC %run ../_SCHEMA/s_finance.gl_segments

# COMMAND ----------

# LOAD DATASETS
if incremental:
  cutoff_value = get_cutoff_value(table_name, 'ebs.fnd_flex_values', prune_days)
  fnd_flex_values = load_incr_dataset('ebs.fnd_flex_values', '_MODIFIED', cutoff_value)
else:
  fnd_flex_values = load_full_dataset('ebs.fnd_flex_values')
  # VIEWS
fnd_flex_values.createOrReplaceTempView('fnd_flex_values')

# COMMAND ----------

# SAMPLING
if sampling:
  fnd_flex_values = fnd_flex_values.limit(10)
  fnd_flex_values.createOrReplaceTempView('fnd_flex_values')

# COMMAND ----------

main = spark.sql("""SELECT    
    Replace(String(INT(MAX(FND_FLEX_VALUES.CREATED_BY))),"," , "")      AS createdBy,
    CASE 
	WHEN YEAR(MAX(FND_FLEX_VALUES.CREATION_DATE)) < 1900
	 THEN CAST(CAST(YEAR(MAX(FND_FLEX_VALUES.CREATION_DATE) ) + 1900 AS STRING) || '-' || DATE_FORMAT(CAST(MAX(FND_FLEX_VALUES.CREATION_DATE) AS DATE), 'MM-dd') AS DATE)
	ELSE MAX(FND_FLEX_VALUES.CREATION_DATE)  END AS createdOn,
    Replace(String(INT(MAX(FND_FLEX_VALUES.LAST_UPDATED_BY))),"," , "")        AS modifiedBy,
    MAX(FND_FLEX_VALUES.LAST_UPDATE_DATE)      AS modifiedOn,
    CURRENT_TIMESTAMP()                        AS insertedOn,
    CURRENT_TIMESTAMP()                        AS updatedOn,
    MAX(FND_FLEX_VALUES.END_DATE_ACTIVE)       AS endDateActive,
    Replace(String(INT(FND_FLEX_VALUE_SETS.FLEX_VALUE_SET_ID)),"," , "") ||'-'||  FND_FLEX_VALUES.FLEX_VALUE segmentId,
    FND_FLEX_VALUE_SETS.FLEX_VALUE_SET_NAME    AS segmentLovName,
    Replace(String(INT(FND_FLEX_VALUE_SETS.FLEX_VALUE_SET_ID)),"," , "")       AS segmentLovId,
    FND_FLEX_VALUES.FLEX_VALUE                 AS segmentValueCode,
    MAX(FND_FLEX_VALUES_TL.DESCRIPTION)        AS segmentValueDescription,
    MAX(FND_FLEX_VALUES.START_DATE_ACTIVE)     AS startDateActive 
    
FROM
    fnd_flex_values
    inner join  ebs.fnd_flex_value_sets on  fnd_flex_values.flex_value_set_id = fnd_flex_value_sets.flex_value_set_id
    inner join  ebs.fnd_flex_values_tl  on fnd_flex_values.flex_value_id = fnd_flex_values_tl.flex_value_id
WHERE
fnd_flex_values_tl.language = 'US'
   
GROUP BY
    fnd_flex_value_sets.flex_value_set_id,
    fnd_flex_value_sets.flex_value_set_name,
    fnd_flex_values.flex_value,
    fnd_flex_value_sets.last_update_date""")
main.display()

# COMMAND ----------

# VALIDATE NK
valid_count_rows(main, 'segmentId')

# COMMAND ----------

columns = list(schema.keys())

# COMMAND ----------

# TRANSFORM DATA
main_f = (
  main
  .transform(tg_default(source_name))
  .transform(tg_finance_gl_segments())
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

# DATA QUALITY
show_null_values(main_f)

# COMMAND ----------

# PERSIST DATA
options = {'target_storage': target_storage, 'target_container': target_container}
merge_to_delta(main_f, table_name, target_folder, overwrite, options = options)

# COMMAND ----------

# HANDLE DELETE
full_keys_f =  (
  spark.sql("""SELECT   
     Replace(String(INT(FND_FLEX_VALUE_SETS.FLEX_VALUE_SET_ID)),"," , "")  || '-' ||      FND_FLEX_VALUES.FLEX_VALUE  AS segmentId
    FROM
       fnd_flex_values
       inner join  ebs.fnd_flex_value_sets on  fnd_flex_values.flex_value_set_id = fnd_flex_value_sets.flex_value_set_id
       inner join  ebs.fnd_flex_values_tl  on fnd_flex_values.flex_value_id = fnd_flex_values_tl.flex_value_id
    WHERE
fnd_flex_values_tl.language = 'US' """)
.transform(attach_source_column(source = source_name))
.transform(attach_surrogate_key(columns = 'segmentId,_Source'))
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
  cutoff_value = get_incr_col_max_value(fnd_flex_values,'_MODIFIED')
  update_cutoff_value(cutoff_value, table_name, 'ebs.fnd_flex_values')
  update_run_datetime(run_datetime, table_name, 'ebs.fnd_flex_values')

# COMMAND ----------

# MAGIC %run ../_SHARED/PARTIALS/1.2/footer_s_finance
