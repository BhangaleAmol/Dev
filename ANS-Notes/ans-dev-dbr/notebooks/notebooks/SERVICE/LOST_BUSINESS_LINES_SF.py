# Databricks notebook source
# MAGIC %run ../_SHARED/FUNCTIONS/1.2/bootstrap

# COMMAND ----------

# MAGIC %run ../_SHARED/PARTIALS/1.2/header_s_service

# COMMAND ----------

# MAGIC %run ../_SCHEMA/s_service.lost_business_lines

# COMMAND ----------

change_path = True

# COMMAND ----------

# LOAD DATASETS
if incremental:
  cutoff_value = get_cutoff_value(table_name, 'sf.Lost_Business_Products__c', prune_days)
  lostbusinessline = load_incr_dataset('sf.Lost_Business_Products__c', 'LastModifiedDate', cutoff_value)
else:
  lostbusinessline = load_full_dataset('sf.Lost_Business_Products__c')

# COMMAND ----------

# SAMPLING
if sampling:
  lostbusinessline = lostbusinessline.limit(10)

# COMMAND ----------

# VIEWS
lostbusinessline.createOrReplaceTempView('lostbusinessline')

# COMMAND ----------

main = spark.sql("""
SELECT
lostbusinessline.CreatedDate AS createdOn,
lostbusinessline.CreatedById AS createdBy,
lostbusinessline.LastModifiedDate AS modifiedOn,
lostbusinessline.LastModifiedById AS modifiedBy,
CURRENT_DATE AS insertedOn,
CURRENT_DATE AS updatedOn,
lostbusinessline.CurrencyIsoCode currencyIsoCode,
lostbusinessline.Id AS lostBusinessLineId,
lostbusinessline.IsDeleted isDeleted,
lostbusinessline.Lost_Business__c AS lostBusinessId,
lostbusinessline.Name name,
lostbusinessline.Product__c AS itemid,
lostbusinessline.Product_Forecast_Group__c AS productForecastGroup,
lostbusinessline.Quantity__c AS quantity,
lostbusinessline.Sales_Price__c AS salesPrice,
lostbusinessline.Total_Value__c AS totalValue
   FROM
   lostbusinessline 
  
""")

main.createOrReplaceTempView('main')

# COMMAND ----------

# VALIDATE NK
valid_count_rows(main, 'lostBusinessLineId')

# COMMAND ----------

# TRANSFORM DATA
main_f = (
  main
  .transform(tg_default(source_name))
  .transform(tg_service_lost_business_lines())
  .transform(apply_schema(schema))
)

# main_f.cache()

# COMMAND ----------

# VALIDATE DATA
valid_count_rows(main_f, '_ID')

# COMMAND ----------

# PERSIST DATA
options = {'target_storage': target_storage, 'target_container': target_container}
merge_to_delta(main_f, table_name, target_folder, overwrite, options = options)
add_unknown_record(main_f, table_name)

# COMMAND ----------

# HANDLE DELETE
full_keys_f = (
  spark.table('sf.Lost_Business_Products__c')
  .selectExpr('Id lostBusinessLineId')
  .transform(attach_source_column(source = source_name))
  .transform(attach_primary_key('lostbusinessLineId,_SOURCE', 'edm.lost_business_lines'))
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
  cutoff_value = get_incr_col_max_value(lostbusinessline, "LastModifiedDate")
  update_cutoff_value(cutoff_value, table_name, 'sf.Lost_Business_Products__c')
  update_run_datetime(run_datetime, table_name, 'sf.Lost_Business_Products__c')

# COMMAND ----------

# MAGIC %run ../_SHARED/PARTIALS/1.2/footer_s_service
