# Databricks notebook source
# MAGIC %run ../_SHARED/FUNCTIONS/1.2/bootstrap

# COMMAND ----------

# MAGIC %run ../_SHARED/PARTIALS/1.2/header_s_service

# COMMAND ----------

# MAGIC %run ../_SCHEMA/s_service.opportunity_lines

# COMMAND ----------

change_path = True

# COMMAND ----------

# LOAD DATASETS
if incremental:
  cutoff_value = get_cutoff_value(table_name, 'sf.opportunitylineitem', prune_days)
  opportunityline = load_incr_dataset('sf.opportunitylineitem', 'LastModifiedDate', cutoff_value)
else:
  opportunityline = load_full_dataset('sf.opportunitylineitem')

# COMMAND ----------

# SAMPLING
if sampling:
  opportunityline = opportunityline.limit(10)

# COMMAND ----------

# VIEWS
opportunityline.createOrReplaceTempView('opportunityline')

# COMMAND ----------

main = spark.sql("""
SELECT
oppline.CreatedDate AS createdOn,
oppline.CreatedById AS createdBy,
oppline.LastModifiedDate AS modifiedOn,
oppline.LastModifiedById AS modifiedBy,
CURRENT_DATE AS insertedOn,
CURRENT_DATE AS updatedOn,
oppline.Annual_Usage__c AS annualUsage,
NULL AS ansellStdUom,
NULL AS avgDmd,
NULL AS avgDpQty,
NULL AS avgFrct,
oppline.Case_Quantity__c AS caseQuantity,
NULL AS changeFlag,
oppline.ConversionBar__c AS conversionBar,
oppline.ConvertedPercentage__c AS convertedPercentage,
oppline.CurrencyIsoCode AS currencyIsoCode,
oppline.Demand_Planning_Quantity__c AS demandPlanningQuantity,
oppline.Description AS description,
oppline.Discount AS discount,
oppline.End_User_Price__c AS endUserPrice,
oppline.Forecast_Quantity__c AS forcastQuantity,
oppline.Forecast_UOM__c AS forecastUom,
NULL AS frctPercent,
oppline.HasQuantitySchedule AS hasQuantitySchedule,
oppline.HasRevenueSchedule AS hasRevenueSchedule,
oppline.HasSchedule AS hasSchedule,
NULL AS implementationDate,
oppline.IsDeleted AS isDeleted,
oppline.LastModifiedDate AS lastModifiedDate,
NULL AS lcs,
oppline.ListPrice AS currency,
oppline.Lost_Business_amount__c AS lostBusinessAmount,
oppline.Lost_business_reason__c AS lostBusinessReason,
CASE WHEN opp.iswon = 'false' AND  opp.isclosed = 'true' THEN oppline.subtotal end AS lostRevenue,
CASE WHEN opp.iswon = 'false' AND  opp.isclosed = 'true' THEN oppline.subtotal_usd end AS lostRevenueUsd,
oppline.Lowest_Shippable_UOM__c AS lowestShippableUom,
oppline.MI_Report_Month__c AS miReportMonth,
NULL AS monthQuantity,
NULL AS mtoFlag,
NULL AS npdFlag,
oppline.One_time_Order__c AS oneTimeOrder,
CASE WHEN opp.isclosed = 'false' THEN oppline.subtotal end AS openRevenue,
CASE WHEN opp.isclosed = 'false' THEN oppline.subtotal_usd end AS openRevenueUsd,
opp.id AS opportunityId,
oppline.Id AS optyProductId,
oppline.PricebookEntryId AS priceBookEntryId,
NULL AS priorCloseDate,
NULL AS priorQuantity,
NULL AS priorStage,
oppline.Product_Description__c AS productDescription,
oppline.Product_Forecast_Category__c AS productForecastCategory,
oppline.Product2id AS productId,
oppline.Product_Monthly_Quantity__c AS productMonthlyQuantity,
oppline.Prorated_Forecast_Quantity__c AS proratedForecastQuantity,
NULL AS qtyInPieces,
oppline.Quantity AS quantity,
oppline.Reminder__c AS reminder,
oppline.Quantity*oppline.UnitPrice AS revenue,
oppline.Subtotal_usd AS revenueUsd,
oppline.ServiceDate AS serviceDate,
NULL AS shipmentType,
oppline.SortOrder AS sortOrder,
oppline.Style_Size__c AS styleSize,
oppline.Subtotal AS subTotal,
oppline.Suggested_Price__c AS suggestedPrice,
oppline.SystemModstamp AS systemModStamp,
oppline.TargetedAmount__c AS targetedAmount,
oppline.TotalPrice AS totalPrice,
oppline.UnitPrice AS unitPrice,
oppline.Subtotal_usd/oppline.quantity AS unitPriceUsd,
oppline.UOM__c AS uom,
oppline.UOM_New__c AS uomNew,
oppline.Update_Price__c AS updatedPrice,
CASE WHEN opp.iswon = 'true' and opp.isclosed = 'true' THEN oppline.subtotal end AS wonRevenue,
CASE WHEN opp.iswon = 'true' and opp.isclosed = 'true' THEN oppline.subtotal_usd end AS wonRevenueUsd
   FROM
   opportunityline oppline
   join sf.opportunity opp on opp.id =  oppline.OpportunityId

  
""")

main.createOrReplaceTempView('main')

# COMMAND ----------

# VALIDATE NK
valid_count_rows(main, 'optyProductId')

# COMMAND ----------

# TRANSFORM DATA
main_f = (
  main
  .transform(tg_default(source_name))
  .transform(tg_service_opportunity_lines())
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
  spark.table('sf.opportunitylineitem')
  .selectExpr('Id optyProductId')
  .transform(attach_source_column(source = source_name))
  .transform(attach_surrogate_key(columns = 'optyProductId,_SOURCE'))
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
  cutoff_value = get_incr_col_max_value(opportunityline, "LastModifiedDate")
  update_cutoff_value(cutoff_value, table_name, 'sf.opportunitylineitem')
  update_run_datetime(run_datetime, table_name, 'sf.opportunitylineitem')

# COMMAND ----------

# MAGIC %run ../_SHARED/PARTIALS/1.2/footer_s_service
