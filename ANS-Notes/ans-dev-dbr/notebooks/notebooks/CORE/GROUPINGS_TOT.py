# Databricks notebook source
# MAGIC %run ../_SHARED/FUNCTIONS/1.2/bootstrap

# COMMAND ----------

# MAGIC %run ../_SHARED/PARTIALS/1.2/header_s_core

# COMMAND ----------

# MAGIC %run ../_SCHEMA/s_core.groupings

# COMMAND ----------

# LOAD DATASETS
tb_pbi_businessgroup = load_full_dataset('tot.tb_pbi_businessgroup')
tb_pbi_productgroup = load_full_dataset('tot.tb_pbi_productgroup')

# COMMAND ----------

# SAMPLING
if sampling:
  tb_pbi_businessgroup = tb_pbi_businessgroup.limit(10)
  tb_pbi_productgroup = tb_pbi_productgroup.limit(10)

# COMMAND ----------

# VIEWS
tb_pbi_businessgroup.createOrReplaceTempView('tb_pbi_businessgroup')
tb_pbi_productgroup.createOrReplaceTempView('tb_pbi_productgroup')

# COMMAND ----------

main = spark.sql("""
  SELECT 
	CAST(NULL AS STRING) 								AS createdBy,
	CAST(NULL AS TIMESTAMP) 							AS createdOn,
	CAST(NULL AS STRING) 								AS modifiedBy,
	CAST(NULL AS TIMESTAMP) 							AS modifiedOn,
	CURRENT_TIMESTAMP() 								AS insertedOn,
	CURRENT_TIMESTAMP() 								AS updatedOn,
	--CONCAT(bg.Company, '-', bg.Business_group_cod) 	    AS groupId,
    'BusinessGroup' || '-' || bg.Company || '-' || bg.Business_group_cod AS groupId,
	'BusinessGroup' 									AS groupType,
	UPPER(bg.Business_group_des) 						AS name      
  FROM tb_pbi_businessgroup bg
  UNION
  SELECT
	CAST(NULL AS STRING) 								AS createdBy,
	CAST(NULL AS TIMESTAMP) 							AS createdOn,
	CAST(NULL AS STRING) 								AS modifiedBy,
	CAST(NULL AS TIMESTAMP) 							AS modifiedOn,
	CURRENT_TIMESTAMP() 								AS insertedOn,
	CURRENT_TIMESTAMP() 								AS updatedOn,
	--CONCAT(pg.Company, '-', pg.Product_group_cod) 		AS groupId,
    'ProductGroup' || '-' || pg.Company || '-' ||  pg.Product_group_cod AS groupId,
	'ProductGroup' 				    					AS groupType,
	UPPER(pg.Product_group_des) 						AS name      
  FROM tb_pbi_productgroup pg
""")

display(main)

# COMMAND ----------

columns = list(schema.keys())

# COMMAND ----------

main = remove_duplicate_rows(df = main, 
                             key_columns = ['groupId','groupType'], 
                             tableName = table_name, 
                             sourceName = source_name, 
                             notebookName = NOTEBOOK_NAME, 
                             notebookPath = NOTEBOOK_PATH)

# COMMAND ----------

# TRANSFORM DATA
main_f = (
  main
  .transform(tg_default(source_name))
  .transform(tg_core_groupings())
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
full_bg_keys = (
  spark.table('tot.tb_pbi_businessgroup')
  .filter('_DELETED IS FALSE')
  .selectExpr("'BusinessGroup' || '-' || Company || '-' || Business_group_cod AS groupId")
  .select('groupId')
)

full_pg_keys = (
  spark.table('tot.tb_pbi_productgroup')
  .filter('_DELETED IS FALSE')
  .selectExpr("'ProductGroup' || '-' || Company || '-' ||  Product_group_cod AS groupId")
  .select('groupId')
)

full_keys_f = (
  full_bg_keys
  .union(full_pg_keys)
  .transform(attach_source_column(source = source_name))
  .transform(attach_surrogate_key(columns = 'groupId,_SOURCE'))
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
  cutoff_value = get_incr_col_max_value(tb_pbi_businessgroup)
  update_cutoff_value(cutoff_value, table_name, 'tot.tb_pbi_businessgroup')
  update_run_datetime(run_datetime, table_name, 'tot.tb_pbi_businessgroup')

  cutoff_value = get_incr_col_max_value(tb_pbi_productgroup)
  update_cutoff_value(cutoff_value, table_name, 'tot.tb_pbi_productgroup')
  update_run_datetime(run_datetime, table_name, 'tot.tb_pbi_productgroup')

# COMMAND ----------

# MAGIC %run ../_SHARED/PARTIALS/1.2/footer_s_core
