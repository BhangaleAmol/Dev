# Databricks notebook source
# MAGIC %run ../_SHARED/FUNCTIONS/1.2/bootstrap

# COMMAND ----------

# MAGIC %run ../_SHARED/PARTIALS/1.2/header_s_service

# COMMAND ----------

# MAGIC %run ../_SCHEMA/s_service.quality_complaint

# COMMAND ----------

# LOAD DATASETS
if incremental:
  cutoff_value = get_cutoff_value(table_name, 'twd.complaint', prune_days)
  complaint = load_incr_dataset('twd.complaint', 'SystemModstamp', cutoff_value)
else:
  complaint = load_full_dataset('twd.complaint')

plantresponsetime = load_full_dataset('twd.plantresponsetime')

# COMMAND ----------

spark.conf.set("spark.sql.legacy.timeParserPolicy", "CORRECTED")

# COMMAND ----------

# SAMPLING
if sampling:
  complaint = complaint.limit(10)
  plantresponsetime = plantresponsetime.limit(10)

# COMMAND ----------

# VIEWS
complaint.createOrReplaceTempView('complaint')
plantresponsetime.createOrReplaceTempView('plantresponsetime')

# COMMAND ----------

DATE_OPEN = spark.sql("""
SELECT 
  CMPL123__RELATED_ID__C COMPLAINTID,
  TO_TIMESTAMP(MIN(CREATEDDATE), 'yyyy-MM-dd HH:mm:ss') FIRST_OPEN_DATE,
  TO_TIMESTAMP(MAX(CREATEDDATE), 'yyyy-MM-dd HH:mm:ss') LAST_OPEN_DATE
FROM twd.workflowhistory
WHERE lower(CMPL123__Status__c) like 'open%'
GROUP BY CMPL123__RELATED_ID__C
""")
DATE_OPEN.createOrReplaceTempView("DATE_OPEN")
DATE_OPEN.cache()
DATE_OPEN.count()

# COMMAND ----------

DATE_EVALUATION = spark.sql("""
SELECT 
  CMPL123__RELATED_ID__C COMPLAINTID,
  TO_TIMESTAMP(MIN(CREATEDDATE), 'yyyy-MM-dd HH:mm:ss') FIRST_EVALUATION_DATE,
  TO_TIMESTAMP(MAX(CREATEDDATE), 'yyyy-MM-dd HH:mm:ss') LAST_EVALUATION_DATE
FROM twd.workflowhistory
WHERE CMPL123__Status__c = 'Evaluation in Progress'
GROUP BY CMPL123__RELATED_ID__C
""")
DATE_EVALUATION.createOrReplaceTempView("DATE_EVALUATION")
DATE_EVALUATION.cache()
DATE_EVALUATION.count()

# COMMAND ----------

DATE_INVESTIGATION = spark.sql("""
SELECT 
  CMPL123__RELATED_ID__C COMPLAINTID,
  TO_TIMESTAMP(MIN(CREATEDDATE), 'yyyy-MM-dd HH:mm:ss') FIRST_INVESTIGATION_DATE,
  TO_TIMESTAMP(MAX(CREATEDDATE), 'yyyy-MM-dd HH:mm:ss') LAST_INVESTIGATION_DATE
FROM twd.workflowhistory
WHERE 1=1
 and CMPL123__Status__c = 'Investigation in Progress'
GROUP BY CMPL123__RELATED_ID__C
""")
DATE_INVESTIGATION.createOrReplaceTempView("DATE_INVESTIGATION")
DATE_INVESTIGATION.cache()
DATE_INVESTIGATION.count()

# COMMAND ----------

DATE_PENDING_APPROVAL = spark.sql("""
SELECT 
  CMPL123__RELATED_ID__C COMPLAINTID,
  TO_TIMESTAMP(MIN(CREATEDDATE), 'yyyy-MM-dd HH:mm:ss') FIRST_APPROVAL_DATE,
  TO_TIMESTAMP(MAX(CREATEDDATE), 'yyyy-MM-dd HH:mm:ss') LAST_APPROVAL_DATE
FROM twd.workflowhistory
WHERE CMPL123__Status__c = 'Pending Approval'
GROUP BY CMPL123__RELATED_ID__C
""")
DATE_PENDING_APPROVAL.createOrReplaceTempView("DATE_PENDING_APPROVAL")
DATE_PENDING_APPROVAL.cache()
DATE_PENDING_APPROVAL.count()

# COMMAND ----------

DATE_APPROVED = spark.sql("""
SELECT 
  CMPL123__RELATED_ID__C COMPLAINTID,
  TO_TIMESTAMP(MIN(CREATEDDATE), 'yyyy-MM-dd HH:mm:ss') FIRST_APPROVED_DATE,
  TO_TIMESTAMP(MAX(CREATEDDATE), 'yyyy-MM-dd HH:mm:ss') LAST_APPROVED_DATE
FROM twd.workflowhistory
WHERE CMPL123__Status__c = 'Approved - pending correspondence'
GROUP BY CMPL123__RELATED_ID__C
""")
DATE_APPROVED.createOrReplaceTempView("DATE_APPROVED")
DATE_APPROVED.cache()
DATE_APPROVED.count()

# COMMAND ----------

DATE_CLOSED = spark.sql("""
SELECT 
  CMPL123__RELATED_ID__C COMPLAINTID,
  TO_TIMESTAMP(MIN(CREATEDDATE), 'yyyy-MM-dd HH:mm:ss') FIRST_CLOSEDATE,
  TO_TIMESTAMP(MAX(CREATEDDATE), 'yyyy-MM-dd HH:mm:ss') LAST_CLOSEDATE
FROM twd.workflowhistory
WHERE lower(CMPL123__Status__c) like  'closed%'
GROUP BY CMPL123__RELATED_ID__C
""")
DATE_CLOSED.createOrReplaceTempView("DATE_CLOSED")
DATE_CLOSED.cache()
DATE_CLOSED.count()

# COMMAND ----------

main_complaint = spark.sql("""
SELECT
  c.CreatedById createdBy,
  c.CreatedDate createdOn,
  c.LastModifiedById modifiedBy,
  c.LastModifiedDate modifiedOn,
  CURRENT_TIMESTAMP() insertedOn,
  CURRENT_TIMESTAMP() updatedOn,
  c.ID complaintId,
  IF(
    c.Customer_Country__c = 'All',
    NULL,
    c.Customer_Country__c
  ) address1Country,
  c.Customer_Address__c address1Line1,
  c.CMPL123CME__Assigned_To__c assignedToId,
  c.CMPL123CME__Business_Risk__c businessRisk,
  c.CaseId__c caseId,
  c.Address__c complaintAddress,
  c.Complaint_Review_and_Approval__c complaintApproverId,
  c.CAPA_ansell__c complaintCapaId,
  CASE
    WHEN c.CAPA_Required__c = 'Yes' THEN TRUE
    WHEN c.CAPA_Required__c = 'No' THEN FALSE
  END complaintCapaRequired,
  c.CMPL123CME__Compliance_Risk__c complaintComplianceRisk,
  c.Country__c complaintContactCountry,
  c.Contact_Name__c complaintContactName,
  c.TWD_Contact_Notes__c complaintContactNotes,
  c.CMPL123CME__Description__c complaintDescription,
  c.Email__c complaintEmail,
  c.TWD_Evaluation_Completed_Date__c complaintEvaluationDate,
  c.CMPL123CME__Health_Risk__c complaintHealthRisk,
  c.TWD_Impact__c complaintImpact,
  c.CMPL123CME__Impact_Analysis__c complaintImpactAnalysis,
  CASE
    WHEN c.TWD_Investigation_Required__c = 'Yes' THEN TRUE
    WHEN c.TWD_Investigation_Required__c = 'No' THEN FALSE
  END complaintInvestigationRequired,
  c.Complaint_Investigator__c complaintInvestigatorId,
  c.CMPL123CME__CMPL123_WF_Action__c complaintLastAction,
  c.NAME complaintName,
  c.No_CAPA_Justification__c complaintNoCapaJustification,
  c.TWD_Phone_1__c complaintPhone1,
  c.CMPL123CME__Product__c complaintProductId,
  c.Affected_Quantity_of_Issue__c complaintQuantity,
  c.TWD_Justification_for_no_Investigation__c complaintReasonForNoInvestigation,
  c.Customer_Response_Required__c complaintResponseRequired,
  c.CMPL123CME__Risk_Analysis__c complaintRiskAnalysis,
  CASE
    WHEN c.Defect_Samples_Available__c = 'Yes' THEN TRUE
    WHEN c.Defect_Samples_Available__c = 'No' THEN FALSE
  END complaintSamplesAvalaible,
  c.CMPL123CME__Short_Description__c complaintShortDescription,
  c.CMPL123CME__CMPL123_WF_Status__c complaintState,
  c.Complaint_Type_Count__c complaintTypeCount,
  c.Affected_Quantity_Units__c complaintUnitOfMeasure,
  c.Customer1__c customerName,
  TO_DATE(c.Date_of_Incident__c, 'yyyy-MM-dd HH:mm:ss') dateOfIncident,
  c.TWD_Due_Date__c dueDate,  
  CASE
    WHEN c.SBU__c IN ('CHEM', 'BP', 'MECH') THEN 'IND'
    WHEN c.SBU__c IN ('SURG', 'EXAM', 'LSS') THEN 'HC'
    WHEN c.GBU__c = 'IGBU' THEN 'IND'
    WHEN c.GBU__c = 'HGBU' THEN 'HC'
    ELSE c.GBU__c
  END gbu,
  c.IsDeleted isDeleted,
  c.Lot_Number__c lotNumber,
  c.Lot_Number_Count__c lotNumberCount,
  c.Manufacturing_Site__c manufacturingSite,
  c.Optional_Approver__c optionalApproverId,
  c.OwnerID ownerId,
  c.Phone__c phone,
  c.Region_Countries__c region,
  c.RMA_RGA_Quantity_To_Return_Or_Replace__c rmargaReturnOrReplace,
  c.RMA_RGA_Quantity_Of_Return_To_Sample__c rmargaReturnToSample,
  c.Sales_Contact__c salesContact,
  c.SBU__c sbu,
  c.SFDC_Creator__c sfdcCreator,
  c.TWD_Email__c twdEmail,
  do.FIRST_OPEN_DATE openDate,
  de.FIRST_EVALUATION_DATE evaluationStartDate,
  de.LAST_EVALUATION_DATE evaluationEndDate,
  di.FIRST_INVESTIGATION_DATE investigationStartDate,
  di.LAST_INVESTIGATION_DATE investigationEndDate,
  dpa.FIRST_APPROVAL_DATE qaApprovalStartDate,
  dpa.LAST_APPROVAL_DATE qaApprovalEndDate,
  da.FIRST_APPROVED_DATE approvedStartDate,
  da.LAST_APPROVED_DATE approvedEndDate,
  dc.last_closedate closedDate,
  ROUND(
    (
      CAST(dpa.LAST_APPROVAL_DATE AS LONG) - CAST(di.FIRST_INVESTIGATION_DATE AS LONG)
    ) / (3600 * 24),
    1
  ) AS investigationTime,
  ROUND(
    (
      CAST(da.LAST_APPROVED_DATE AS LONG) - CAST(di.FIRST_INVESTIGATION_DATE AS LONG)
    ) / (3600 * 24),
    1
  ) AS plantResponseTime,
  ROUND(
    (
      CAST(dc.LAST_CLOSEDATE AS LONG) - CAST(da.FIRST_APPROVED_DATE AS LONG)
    ) / (3600 * 24),
    1
  ) AS caResponseTime,
  ROUND(
    (
      COALESCE(CAST(da.LAST_APPROVED_DATE AS LONG) - CAST(di.FIRST_INVESTIGATION_DATE AS LONG), 0) + 
      COALESCE(CAST(dc.LAST_CLOSEDATE AS LONG) - CAST(da.FIRST_APPROVED_DATE AS LONG), 0)
    ) / (3600 * 24),
    1
  ) AS totalResponseTime,
  CASE
    WHEN cmpl123cme__cmpl123_wf_status__c LIKE 'Cancelled%'
    OR cmpl123cme__cmpl123_wf_status__c LIKE 'Closed%' THEN DATEDIFF(dc.last_closedate, Createddate)
    ELSE DATEDIFF(CURRENT_DATE, Createddate)
  END daysOpen,
  'FY' || DATE_FORMAT(ADD_MONTHS(c.CreatedDate, 6), 'yy') fiscalYear,
  c._DELETED
FROM
  complaint c
  LEFT OUTER JOIN DATE_OPEN do ON c.id = do.complaintid
  LEFT OUTER JOIN DATE_EVALUATION de ON c.id = de.complaintid
  LEFT OUTER JOIN DATE_INVESTIGATION di ON c.id = di.complaintid
  LEFT OUTER JOIN DATE_PENDING_APPROVAL dpa ON c.id = dpa.complaintid
  left outer join DATE_APPROVED da ON c.id = da.complaintid
  LEFT OUTER JOIN DATE_CLOSED dc ON c.id = dc.complaintid
""")

main_complaint.createOrReplaceTempView('main_complaint')

# COMMAND ----------

# TRANSFORM DATA
main_complaint_f = (
  main_complaint
  .transform(parse_timestamp(['createdOn'], expected_format = 'yyyy-MM-dd')) 
)

main_complaint_f.cache()
main_complaint_f.display()

# COMMAND ----------

main_plantresponsetime = spark.sql("""
SELECT 
  'System'                     createdBy,
  to_timestamp(MonthCreated, 'yyyy-MM-dd') createdOn,
  'System'                     modifiedBy,
  to_timestamp(MonthCreated, 'yyyy-MM-dd') modifiedOn,
  CURRENT_TIMESTAMP() insertedOn,
  CURRENT_TIMESTAMP() updatedOn,
  RecordID                     complaintId,
  IF (Country = 'All', NULL, Country) address1Country,
  NULL                           address1Line1,
  NULL                           assignedToId,
  NULL                           businessRisk,
  NULL                           caseId,
  NULL                           complaintAddress,
  NULL                           complaintApproverId,
  NULL                           complaintCapaId,
  NULL                           complaintCapaRequired,
  NULL                           complaintComplianceRisk,
  NULL                           complaintContactCountry,
  NULL                           complaintContactName,
  NULL                           complaintContactNotes,
  NULL                           complaintDescription, 
  NULL                           complaintEmail, 
  NULL                           complaintEvaluationDate,
  NULL                           complaintHealthRisk,
  NULL                           complaintImpact,
  NULL                           complaintImpactAnalysis,
  NULL                           complaintInvestigationRequired,
  NULL                           complaintInvestigatorId,
  NULL                           complaintLastAction,
  RecordID                       complaintName,
  NULL                           complaintNoCapaJustification,
  NULL                           complaintPhone1,
  NULL                           complaintProductId,
  NULL                           complaintQuantity,
  NULL                           complaintReasonForNoInvestigation,
  FALSE                          complaintResponseRequired,
  NULL                           complaintRiskAnalysis,
  NULL                           complaintSamplesAvalaible,
  NULL                           complaintShortDescription,
  NULL                           complaintState,
  NULL                           complaintTypeCount,
  NULL                           complaintUnitOfMeasure,
  NULL                           customerName,
  NULL                           dateOfIncident,
  NULL                           dueDate,  
  CASE
    WHEN SBU IN ('CHEM', 'BP', 'MECH') THEN 'IND'
    WHEN SBU IN ('SURG', 'EXAM', 'LSS') THEN 'HC'
    ELSE NULL
  END gbu,  
  FALSE                        isDeleted,
  NULL                           lotNumber,
  NULL                           lotNumberCount,
  OriginatingMfg               manufacturingSite,
  NULL                           optionalApproverId,
  NULL                           ownerID,
  NULL                           phone,
  CASE 
    WHEN Country IN ('USA', 'United States', 'Canada')
      THEN 'NA'
    WHEN Country IN ('Japan', 'China')  
      THEN 'APAC'
    WHEN Country IN ('All')  
      THEN NULL
    ELSE Region
  END                          region,
  NULL                           rmargaReturnOrReplace,
  NULL                           rmargaReturnToSample,
  NULL                           salesContact,
  SBU                          sbu,
  NULL                           sfdcCreator,
  NULL                           twdEmail,
  MonthCreated                 openDate,
  NULL                           evaluationStartDate,
  NULL                           evaluationEndDate,
  NULL                           investigationStartDate,
  NULL                           investigationEndDate,
  IF(Qapproval = 'NaT', NULL, Qapproval)                    qaApprovalStartDate,
  IF(Qapproval = 'NaT', NULL, Qapproval)                    qaApprovalEndDate,
  NULL                           approvedStartDate,
  NULL                           approvedEndDate,
  to_timestamp(monthclosed, 'yyyy-MM-dd') closedDate,
  InvestigationQA              investigationTime,
  PlantResponse      plantResponseTime,
  CAResponse         caResponseTime,
  COALESCE(PlantResponse) + COALESCE(CAResponse)   totalResponseTime,
  Opened                       daysOpen,
  'FY' || DATE_FORMAT(ADD_MONTHS(MonthCreated, 6), 'yy') fiscalYear,
  FALSE _DELETED
FROM plantresponsetime
""")

main_plantresponsetime.createOrReplaceTempView('main_plantresponsetime')

# COMMAND ----------

# TRANSFORM DATA
main_plantresponsetime_f = (
  main_plantresponsetime
  .transform(parse_timestamp(['createdOn'], expected_format = 'yyyy-MM-dd')) 
#   .transform(apply_schema(schema))
)

main_plantresponsetime_f.cache()
main_plantresponsetime_f.display()

# COMMAND ----------

main = main_complaint_f.union(main_plantresponsetime_f)
main.createOrReplaceTempView('main')

# COMMAND ----------

# VALIDATE NK - SHOW EMPTY VALUES
show_empty_rows(main, ['complaintId'])

# COMMAND ----------

# VALIDATE NK - SHOW DUPLICATES
show_duplicate_rows(main, ['complaintId'])

# COMMAND ----------

# VALIDATE NK - DISTINCT COUNT
valid_count_rows(main, ['complaintId'])

# COMMAND ----------

# CHECK DATA QUALITY
show_null_values(main, ['createdBy', 'modifiedBy', 'assignedToId', 'caseId', 'complaintApproverId', 'complaintCapaId', 'complaintInvestigatorId', 'optionalApproverId', 'ownerID'])

# COMMAND ----------

# MAGIC %run ../_MAPS/s_service.quality_complaint

# COMMAND ----------

columns = list(schema.keys())

# COMMAND ----------

# TRANSFORM DATA
main_f = (
  main
  .transform(apply_mappings(mapping))
  .transform(tg_default(source_name))
  .transform(tg_service_quality_complaint())
  .transform(fix_cell_values(main.columns))
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
full_c_keys = (
  spark.table('twd.complaint')
  .filter('_DELETED IS FALSE')
  .selectExpr('ID AS complaintId')
)

full_prt_keys = (
  spark.table('twd.plantresponsetime')
  .filter('_DELETED IS FALSE')
  .selectExpr('RecordID AS complaintId')
)

full_keys_f = (
  full_c_keys
  .union(full_prt_keys)
  .transform(attach_source_column(source = source_name))
  .transform(attach_surrogate_key(columns = 'complaintId,_SOURCE'))
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
  cutoff_value = get_incr_col_max_value(complaint, "SystemModstamp")
  update_cutoff_value(cutoff_value, table_name, 'twd.complaint')
  update_run_datetime(run_datetime, table_name, 'twd.complaint')

# COMMAND ----------

# MAGIC %run ../_SHARED/PARTIALS/1.2/footer_s_service
