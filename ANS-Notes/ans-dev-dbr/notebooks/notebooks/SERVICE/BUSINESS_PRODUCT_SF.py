# Databricks notebook source
# MAGIC %run ../_SHARED/FUNCTIONS/1.2/bootstrap

# COMMAND ----------

# MAGIC %run ../_SHARED/PARTIALS/1.2/header_s_service

# COMMAND ----------

# MAGIC %run ../_SCHEMA/s_service.business_product

# COMMAND ----------

# LOAD DATASETS
if incremental:
  cutoff_value = get_cutoff_value(table_name, 'sf.business_product__c', prune_days)
  businessproduct = load_incr_dataset('sf.business_product__c', 'LastModifiedDate', cutoff_value)
else:
  businessproduct = load_full_dataset('sf.business_product__c')

# COMMAND ----------

# SAMPLING
if sampling:
  businessproduct = businessproduct.limit(10)

# COMMAND ----------

# VIEWS
businessproduct.createOrReplaceTempView('businessproduct')

# COMMAND ----------

main = spark.sql("""
SELECT
  CreatedDate AS createdOn,
  CreatedById AS createdBy,
  LastModifiedDate AS modifiedOn,
  LastModifiedById AS modifiedBy,
  CURRENT_DATE AS insertedOn,
  CURRENT_DATE AS updatedOn,
  Business_Overview__c AS businessOverview,
  Id AS businessproductId,
  CurrencyIsoCode AS currencyisocode,
  IsDeleted AS isDeleted,
  Name AS name,
  Opportunity_Price__c AS opportunityPrice,
  Product__c AS product,
  Product_Code__c AS productCode,
  Product_Description__c AS productDescription,
  Product_GBU__c AS productGbu,
  Product_SBU__c AS productSbu,
  Product_Style__c AS productStyle,
  Quantity__c AS quantity,
  Style_Description__c AS styleDescription,
  Style_Size__c AS styleSize,
  Total_Price__c AS totalPrice,
  Unit_Price__c AS unitPrice,
  UOM__c AS uom
FROM
  businessproduct
  
""")

main.createOrReplaceTempView('main')

# COMMAND ----------

# VALIDATE NK
valid_count_rows(main, 'businessproductId')

# COMMAND ----------

# TRANSFORM DATA
main_f = (
  main
  .transform(tg_default(source_name))
  .transform(tg_service_business_product())
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
  spark.table('sf.business_product__c')
  .selectExpr('Id businessproductId')
  .transform(attach_source_column(source = source_name))
  .transform(attach_primary_key('businessproductId,_source', 'edm.business_product'))
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
  cutoff_value = get_incr_col_max_value(businessproduct, "LastModifiedDate")
  update_cutoff_value(cutoff_value, table_name, 'sf.business_product__c')
  update_run_datetime(run_datetime, table_name, 'sf.business_product__c')

# COMMAND ----------

# MAGIC %run ../_SHARED/PARTIALS/1.2/footer_s_service
