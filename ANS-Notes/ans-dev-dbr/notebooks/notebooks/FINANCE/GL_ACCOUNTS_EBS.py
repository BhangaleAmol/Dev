# Databricks notebook source
# MAGIC %run ../_SHARED/FUNCTIONS/1.2/bootstrap

# COMMAND ----------

# MAGIC %run ../_SHARED/PARTIALS/1.2/header_s_finance

# COMMAND ----------

# MAGIC %run ../_SCHEMA/s_finance.gl_accounts

# COMMAND ----------

# LOAD DATASETS
if incremental:
  cutoff_value = get_cutoff_value(table_name, 'ebs.gl_code_combinations', prune_days)
  gl_code_combinations = load_incr_dataset('ebs.gl_code_combinations', '_MODIFIED', cutoff_value)
else:
  gl_code_combinations = load_full_dataset('ebs.gl_code_combinations')
  # VIEWS
gl_code_combinations.createOrReplaceTempView('gl_code_combinations')

# COMMAND ----------

# SAMPLING
if sampling:
  gl_code_combinations = gl_code_combinations.limit(10)
  gl_code_combinations.createOrReplaceTempView('gl_code_combinations')

# COMMAND ----------

FIN_STMT_LKP = spark.sql("""
SELECT DISTINCT
                  W_GROUP_ACCT_FIN_STMT_D_TMP.FIN_STMT_ITEM_CODE,
                  W_GROUP_ACCT_FIN_STMT_D_TMP.FIN_STMT_ITEM_NAME,
                  W_GROUP_ACCT_FIN_STMT_D_TMP.GL_ACCOUNT_CAT_NAME, 
                  W_GROUP_ACCT_FIN_STMT_D_TMP.GL_ACCT_CAT_CODE,
                  REPLACE(STRING(INT(W_ORA_GROUP_ACCOUNT_NUM_D_TMP.CHART_OF_ACCOUNTS_ID )), ",", "" ) CHART_OF_ACCOUNTS_ID,
                  W_ORA_GROUP_ACCOUNT_NUM_D_TMP.FROM_ACCT_NUM, 
                  W_ORA_GROUP_ACCOUNT_NUM_D_TMP.TO_ACCT_NUM,
                  W_ORA_GROUP_ACCOUNT_NUM_D_TMP.FROM_SEG1,
                  W_ORA_GROUP_ACCOUNT_NUM_D_TMP.TO_SEG1,
                  W_ORA_GROUP_ACCOUNT_NUM_D_TMP.GROUP_ACCT_NUM
              from smartsheets.W_GROUP_ACCT_FIN_STMT_D_TMP 
              JOIN smartsheets.W_ORA_GROUP_ACCOUNT_NUM_D_TMP 
                ON W_ORA_GROUP_ACCOUNT_NUM_D_TMP.GROUP_ACCT_NUM = W_GROUP_ACCT_FIN_STMT_D_TMP.GROUP_ACCT_NUM
""")
FIN_STMT_LKP.createOrReplaceTempView("FIN_STMT_LKP")
FIN_STMT_LKP.count()

# COMMAND ----------

GL_SEGMENTS_LKP = spark.sql("""
SELECT    
    Replace(String(INT(MAX(FND_FLEX_VALUES.CREATED_BY))),"," , "")      AS createdBy,
    MAX(FND_FLEX_VALUES.CREATION_DATE)        AS createdOn,
    Replace(String(INT(MAX(FND_FLEX_VALUES.LAST_UPDATED_BY))),"," , "")        AS modifiedBy,
    MAX(FND_FLEX_VALUES.LAST_UPDATE_DATE)      AS modifiedOn,
    CURRENT_TIMESTAMP()                        AS insertedOn,
    CURRENT_TIMESTAMP()                        AS updatedOn,
    MAX(FND_FLEX_VALUES.END_DATE_ACTIVE)       AS endDateActive,
    FND_FLEX_VALUE_SETS.FLEX_VALUE_SET_NAME    AS segmentLovName,
    Replace(String(INT(FND_FLEX_VALUE_SETS.FLEX_VALUE_SET_ID)),"," , "")       AS segmentLovId,
    FND_FLEX_VALUES.FLEX_VALUE                 AS segmentValueCode,
    MAX(FND_FLEX_VALUES_TL.DESCRIPTION)        AS segmentValueDescription,
    MAX(FND_FLEX_VALUES.START_DATE_ACTIVE)     AS startDateActive 
    
FROM
    ebs.fnd_flex_values
    inner join  ebs.fnd_flex_value_sets on  fnd_flex_values.flex_value_set_id = fnd_flex_value_sets.flex_value_set_id
    inner join  ebs.fnd_flex_values_tl  on fnd_flex_values.flex_value_id = fnd_flex_values_tl.flex_value_id
WHERE
fnd_flex_values_tl.language = 'US'
   
GROUP BY
    fnd_flex_value_sets.flex_value_set_id,
    fnd_flex_value_sets.flex_value_set_name,
    fnd_flex_values.flex_value,
    fnd_flex_value_sets.last_update_date""")
GL_SEGMENTS_LKP.createOrReplaceTempView("SEGMENTS_LKP")
# GL_SEGMENTS_LKP.display()
GL_SEGMENTS_LKP.count()

# COMMAND ----------

GL_ACCOUNTS = spark.sql("""
SELECT
      NULL as createdBy
    , CURRENT_TIMESTAMP() as createdOn
    , REPLACE(STRING(INT(GL_CODE_COMBINATIONS.LAST_UPDATED_BY)), ",", "" ) AS modifiedBy
    , GL_CODE_COMBINATIONS.LAST_UPDATE_DATE AS modifiedOn
    , CURRENT_TIMESTAMP() as insertedOn
    , CURRENT_TIMESTAMP() as updatedOn
    , REPLACE(STRING(INT(GL_CODE_COMBINATIONS.CHART_OF_ACCOUNTS_ID)), ",", "" ) as chartOfAccounts
    , REPLACE(STRING(INT(FND_ID_FLEX_SEGMENTS3.FLEX_VALUE_SET_ID)), ",", "" ) as costCenterAttribute
    , FND_SEGMENT_ATTRIBUTE_VALUES1.APPLICATION_COLUMN_NAME AS costCenterDescription
    , CASE  
          WHEN FND_SEGMENT_ATTRIBUTE_VALUES1.APPLICATION_COLUMN_NAME = 'SEGMENT1'
              THEN GL_CODE_COMBINATIONS.SEGMENT1
          WHEN FND_SEGMENT_ATTRIBUTE_VALUES1.APPLICATION_COLUMN_NAME = 'SEGMENT2'
              THEN GL_CODE_COMBINATIONS.SEGMENT2
          WHEN FND_SEGMENT_ATTRIBUTE_VALUES1.APPLICATION_COLUMN_NAME = 'SEGMENT3'
              THEN GL_CODE_COMBINATIONS.SEGMENT3
          WHEN FND_SEGMENT_ATTRIBUTE_VALUES1.APPLICATION_COLUMN_NAME = 'SEGMENT4'
              THEN GL_CODE_COMBINATIONS.SEGMENT4
          WHEN FND_SEGMENT_ATTRIBUTE_VALUES1.APPLICATION_COLUMN_NAME = 'SEGMENT5'
              THEN GL_CODE_COMBINATIONS.SEGMENT5
          WHEN FND_SEGMENT_ATTRIBUTE_VALUES1.APPLICATION_COLUMN_NAME = 'SEGMENT6'
              THEN GL_CODE_COMBINATIONS.SEGMENT6
      END AS costCenterNumber
    , FND_SEGMENT_ATTRIBUTE_VALUES.APPLICATION_COLUMN_NAME AS glAccountDescription
    , CASE  
          WHEN FND_SEGMENT_ATTRIBUTE_VALUES.APPLICATION_COLUMN_NAME = 'SEGMENT1'
              THEN GL_CODE_COMBINATIONS.SEGMENT1
          WHEN FND_SEGMENT_ATTRIBUTE_VALUES.APPLICATION_COLUMN_NAME = 'SEGMENT2'
              THEN GL_CODE_COMBINATIONS.SEGMENT2
          WHEN FND_SEGMENT_ATTRIBUTE_VALUES.APPLICATION_COLUMN_NAME = 'SEGMENT3'
              THEN GL_CODE_COMBINATIONS.SEGMENT3
          WHEN FND_SEGMENT_ATTRIBUTE_VALUES.APPLICATION_COLUMN_NAME = 'SEGMENT4'
              THEN GL_CODE_COMBINATIONS.SEGMENT4
          WHEN FND_SEGMENT_ATTRIBUTE_VALUES.APPLICATION_COLUMN_NAME = 'SEGMENT5'
              THEN GL_CODE_COMBINATIONS.SEGMENT5
          WHEN FND_SEGMENT_ATTRIBUTE_VALUES.APPLICATION_COLUMN_NAME = 'SEGMENT6'
              THEN GL_CODE_COMBINATIONS.SEGMENT6
      END AS glAccountNumber
    , REPLACE(STRING(INT(GL_CODE_COMBINATIONS.CODE_COMBINATION_ID)), ",", "" ) AS glCombinationId
    , REPLACE(STRING(INT(FND_ID_FLEX_SEGMENTS2.FLEX_VALUE_SET_ID)), ",", "" ) AS profitCenterAttribute
    , FND_SEGMENT_ATTRIBUTE_VALUES2.APPLICATION_COLUMN_NAME  AS profitCenterDescription
    , CASE  
          WHEN FND_SEGMENT_ATTRIBUTE_VALUES2.APPLICATION_COLUMN_NAME = 'SEGMENT1'
              THEN GL_CODE_COMBINATIONS.SEGMENT1
          WHEN FND_SEGMENT_ATTRIBUTE_VALUES2.APPLICATION_COLUMN_NAME = 'SEGMENT2'
              THEN GL_CODE_COMBINATIONS.SEGMENT2
          WHEN FND_SEGMENT_ATTRIBUTE_VALUES2.APPLICATION_COLUMN_NAME = 'SEGMENT3'
              THEN GL_CODE_COMBINATIONS.SEGMENT3
          WHEN FND_SEGMENT_ATTRIBUTE_VALUES2.APPLICATION_COLUMN_NAME = 'SEGMENT4'
              THEN GL_CODE_COMBINATIONS.SEGMENT4
          WHEN FND_SEGMENT_ATTRIBUTE_VALUES2.APPLICATION_COLUMN_NAME = 'SEGMENT5'
              THEN GL_CODE_COMBINATIONS.SEGMENT5
          WHEN FND_SEGMENT_ATTRIBUTE_VALUES2.APPLICATION_COLUMN_NAME = 'SEGMENT6'
              THEN GL_CODE_COMBINATIONS.SEGMENT6
      END AS profitCenterNumber
    , GL_CODE_COMBINATIONS.ACCOUNT_TYPE AS reconciliationTypeCode
    , (select 
            MAX(REPLACE(STRING(INT(SEG1_ATTRIB)), ",", "" )) 
         from smartsheets.W_GLACCT_SEG_CONFIG_TMP 
         where REPLACE(STRING(INT(CHARTFIELD_GROUP_ID)), ",", "" ) = REPLACE(STRING(INT(GL_CODE_COMBINATIONS.CHART_OF_ACCOUNTS_ID)), ",", "" )
         ) segment1Attribute
    , GL_CODE_COMBINATIONS.SEGMENT1 AS segment1Code
    , (select 
            MAX(REPLACE(STRING(INT(SEG2_ATTRIB)), ",", "" )) 
         from smartsheets.W_GLACCT_SEG_CONFIG_TMP 
         where REPLACE(STRING(INT(CHARTFIELD_GROUP_ID)), ",", "" ) = REPLACE(STRING(INT(GL_CODE_COMBINATIONS.CHART_OF_ACCOUNTS_ID)), ",", "" )
         ) segment2Attribute
    , GL_CODE_COMBINATIONS.SEGMENT2 AS segment2Code
    , (select 
            MAX(REPLACE(STRING(INT(SEG3_ATTRIB)), ",", "" )) 
         from smartsheets.W_GLACCT_SEG_CONFIG_TMP 
         where REPLACE(STRING(INT(CHARTFIELD_GROUP_ID)), ",", "" ) = REPLACE(STRING(INT(GL_CODE_COMBINATIONS.CHART_OF_ACCOUNTS_ID)), ",", "" )
         ) segment3Attribute
    ,  GL_CODE_COMBINATIONS.SEGMENT3 AS segment3Code
    , (select 
            MAX(REPLACE(STRING(INT(SEG4_ATTRIB)), ",", "" )) 
         from smartsheets.W_GLACCT_SEG_CONFIG_TMP 
         where REPLACE(STRING(INT(CHARTFIELD_GROUP_ID)), ",", "" ) = REPLACE(STRING(INT(GL_CODE_COMBINATIONS.CHART_OF_ACCOUNTS_ID)), ",", "" )
         ) segment4Attribute
    ,  GL_CODE_COMBINATIONS.SEGMENT4 AS segment4Code
    , (select 
            MAX(REPLACE(STRING(INT(SEG5_ATTRIB)), ",", "" )) 
         from smartsheets.W_GLACCT_SEG_CONFIG_TMP 
         where REPLACE(STRING(INT(CHARTFIELD_GROUP_ID)), ",", "" ) = REPLACE(STRING(INT(GL_CODE_COMBINATIONS.CHART_OF_ACCOUNTS_ID)), ",", "" )
         ) segment5Attribute
    , GL_CODE_COMBINATIONS.SEGMENT5 AS segment5Code
    , (select 
            MAX(REPLACE(STRING(INT(SEG6_ATTRIB)), ",", "" )) 
         from smartsheets.W_GLACCT_SEG_CONFIG_TMP 
         where REPLACE(STRING(INT(CHARTFIELD_GROUP_ID)), ",", "" ) = REPLACE(STRING(INT(GL_CODE_COMBINATIONS.CHART_OF_ACCOUNTS_ID)), ",", "" )
         ) segment6Attribute
    , GL_CODE_COMBINATIONS.SEGMENT6 AS segment6Code
    , (select 
            MAX(REPLACE(STRING(INT(SEG7_ATTRIB)), ",", "" )) 
         from smartsheets.W_GLACCT_SEG_CONFIG_TMP 
         where REPLACE(STRING(INT(CHARTFIELD_GROUP_ID)), ",", "" ) = REPLACE(STRING(INT(GL_CODE_COMBINATIONS.CHART_OF_ACCOUNTS_ID)), ",", "" )
         ) segment7Attribute
    , GL_CODE_COMBINATIONS.SEGMENT7 AS segment7Code
    , GL_CODE_COMBINATIONS.SEGMENT8 AS segment8Code
FROM
    ebs.gl_code_combinations
    JOIN ebs.fnd_segment_attribute_values
      ON fnd_segment_attribute_values.id_flex_num = gl_code_combinations.chart_of_accounts_id
        AND fnd_segment_attribute_values.application_id = 101
        AND fnd_segment_attribute_values.id_flex_code = 'GL#'
        AND fnd_segment_attribute_values.segment_attribute_type = 'GL_ACCOUNT'
        AND fnd_segment_attribute_values.attribute_value = 'Y'
    left join ebs.fnd_segment_attribute_values fnd_segment_attribute_values1
      ON fnd_segment_attribute_values1.id_flex_num  = gl_code_combinations.chart_of_accounts_id
        AND fnd_segment_attribute_values1.application_id  = 101
        AND fnd_segment_attribute_values1.id_flex_code  = 'GL#'
        AND fnd_segment_attribute_values1.segment_attribute_type = 'FA_COST_CTR'
        AND fnd_segment_attribute_values1.attribute_value = 'Y'
    join ebs.fnd_segment_attribute_values fnd_segment_attribute_values2
      ON fnd_segment_attribute_values2.id_flex_num = gl_code_combinations.chart_of_accounts_id
        AND fnd_segment_attribute_values2.application_id = 101
        AND fnd_segment_attribute_values2.id_flex_code = 'GL#'
        AND fnd_segment_attribute_values2.segment_attribute_type = 'GL_BALANCING'
        AND fnd_segment_attribute_values2.attribute_value = 'Y'
    JOIN ebs.fnd_id_flex_segments  -- GL Accounts
      ON fnd_id_flex_segments.id_flex_num = gl_code_combinations.chart_of_accounts_id
        AND fnd_id_flex_segments.id_flex_code = 'GL#'
        AND fnd_id_flex_segments.application_id = 101
        AND fnd_id_flex_segments.application_column_name = fnd_segment_attribute_values.application_column_name   
    JOIN ebs.fnd_id_flex_segments         fnd_id_flex_segments2 -- profit center
      ON fnd_id_flex_segments2.id_flex_num = gl_code_combinations.chart_of_accounts_id
        AND fnd_id_flex_segments2.id_flex_code = 'GL#'
        AND fnd_id_flex_segments2.application_id = 101
        AND fnd_id_flex_segments2.application_column_name = fnd_segment_attribute_values2.application_column_name 
    JOIN ebs.fnd_id_flex_segments         fnd_id_flex_segments3 -- cost center
      ON fnd_id_flex_segments3.id_flex_num = gl_code_combinations.chart_of_accounts_id
        AND fnd_id_flex_segments3.id_flex_code = 'GL#'
        AND fnd_id_flex_segments3.application_id = 101  
        AND fnd_id_flex_segments3.application_column_name = fnd_segment_attribute_values1.application_column_name
    join ebs.fnd_id_flex_structures
      ON fnd_id_flex_structures.id_flex_num = gl_code_combinations.chart_of_accounts_id
        AND fnd_id_flex_structures.id_flex_code = 'GL#'
        AND fnd_id_flex_structures.application_id = 101
        AND fnd_id_flex_structures.enabled_flag = 'Y'
""")
GL_ACCOUNTS.createOrReplaceTempView("GL_ACCOUNTS")
GL_ACCOUNTS.count()

# COMMAND ----------

main = spark.sql("""
select 
        createdBy
      , createdOn
      , modifiedBy
      , modifiedOn
      , insertedOn
      , updatedOn
      , chartOfAccounts
      , costCenterAttribute
      , costCenterDescription
      , costCenterNumber
      , STMT_ITEM.FIN_STMT_ITEM_CODE AS financialStatementItemCode
      , STMT_ITEM.FIN_STMT_ITEM_NAME AS financialStatementItemName
      , STMT_ITEM.GL_ACCT_CAT_CODE AS glAccountCategoryCode
      , STMT_ITEM.GL_ACCOUNT_CAT_NAME AS glAccountCategoryName
      , glAccountDescription
      , glAccountNumber
      , glCombinationId
      , STMT_ITEM.GROUP_ACCT_NUM AS groupAccountName
      , STMT_ITEM.GROUP_ACCT_NUM AS groupAccountNumber
      , profitCenterAttribute
      , profitCenterDescription
      , profitCenterNumber
      , reconciliationTypeCode
      , segment1Attribute
      , segment1Code
      , (select 
            MAX(NVL(segmentLovName,'')) segmentLovName 
         from SEGMENTS_LKP 
         where segmentValueCode = gl_acc.segment1Code 
         and segmentLovId = gl_acc.segment1Attribute 
         ) segment1Name
      , segment2Attribute
      , segment2Code
      , (select 
            MAX(NVL(segmentLovName,'')) segmentLovName 
         from SEGMENTS_LKP 
         where segmentValueCode = gl_acc.segment2Code 
         and segmentLovId = gl_acc.segment2Attribute 
         ) segment2Name
      , segment3Attribute
      , segment3Code
      , (select 
            MAX(NVL(segmentLovName,'')) segmentLovName 
         from SEGMENTS_LKP 
         where segmentValueCode = gl_acc.segment3Code 
         and segmentLovId = gl_acc.segment3Attribute 
         ) segment3Name
      , segment4Attribute
      , segment4Code
      , (select 
            MAX(NVL(segmentLovName,'')) segmentLovName 
         from SEGMENTS_LKP 
         where segmentValueCode = gl_acc.segment4Code 
         and segmentLovId = gl_acc.segment4Attribute 
         ) segment4Name
      , segment5Attribute
      , segment5Code
      , (select 
            MAX(NVL(segmentLovName,'')) segmentLovName 
         from SEGMENTS_LKP 
         where segmentValueCode = gl_acc.segment5Code 
         and segmentLovId = gl_acc.segment5Attribute 
         ) segment5Name
      , segment6Attribute
      , segment6Code
      , (select 
            MAX(NVL(segmentLovName,'')) segmentLovName 
         from SEGMENTS_LKP 
         where segmentValueCode = gl_acc.segment6Code 
         and segmentLovId = gl_acc.segment6Attribute 
         ) segment6Name
      , segment7Attribute
      , segment7Code
      , (select 
            MAX(NVL(segmentLovName,'')) segmentLovName 
         from SEGMENTS_LKP 
         where segmentValueCode = gl_acc.segment7Code 
         and segmentLovId = gl_acc.segment7Attribute 
         ) segment7Name
      , NULL AS segment8Attribute
      , segment8Code
      , NULL AS segment8Name
from GL_ACCOUNTS gl_acc
LEFT JOIN FIN_STMT_LKP STMT_ITEM
      ON STMT_ITEM.CHART_OF_ACCOUNTS_ID = gl_acc.chartOfAccounts
      and gl_acc.glAccountNumber between STMT_ITEM.FROM_ACCT_NUM and STMT_ITEM.TO_ACCT_NUM
      and gl_acc.costCenterNumber between STMT_ITEM.FROM_SEG1 and STMT_ITEM.TO_SEG1
""")
main.count()

# COMMAND ----------

# VALIDATE NK
valid_count_rows(main, 'glCombinationId')

# COMMAND ----------

columns = list(schema.keys())

# COMMAND ----------

# TRANSFORM DATA
main_f = (
  main
  .transform(tg_default(source_name))
  .transform(tg_finance_gl_accounts())
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
     REPLACE(STRING(INT(GL_CODE_COMBINATIONS.CODE_COMBINATION_ID)), ",", "" ) AS glCombinationId
    FROM
    ebs.gl_code_combinations
    JOIN ebs.fnd_segment_attribute_values
      ON fnd_segment_attribute_values.id_flex_num = gl_code_combinations.chart_of_accounts_id
        AND fnd_segment_attribute_values.application_id = 101
        AND fnd_segment_attribute_values.id_flex_code = 'GL#'
        AND fnd_segment_attribute_values.segment_attribute_type = 'GL_ACCOUNT'
        AND fnd_segment_attribute_values.attribute_value = 'Y'
    left join ebs.fnd_segment_attribute_values fnd_segment_attribute_values1
      ON fnd_segment_attribute_values1.id_flex_num  = gl_code_combinations.chart_of_accounts_id
        AND fnd_segment_attribute_values1.application_id  = 101
        AND fnd_segment_attribute_values1.id_flex_code  = 'GL#'
        AND fnd_segment_attribute_values1.segment_attribute_type = 'FA_COST_CTR'
        AND fnd_segment_attribute_values1.attribute_value = 'Y'
    join ebs.fnd_segment_attribute_values fnd_segment_attribute_values2
      ON fnd_segment_attribute_values2.id_flex_num = gl_code_combinations.chart_of_accounts_id
        AND fnd_segment_attribute_values2.application_id = 101
        AND fnd_segment_attribute_values2.id_flex_code = 'GL#'
        AND fnd_segment_attribute_values2.segment_attribute_type = 'GL_BALANCING'
        AND fnd_segment_attribute_values2.attribute_value = 'Y'
    JOIN ebs.fnd_id_flex_segments  -- GL Accounts
      ON fnd_id_flex_segments.id_flex_num = gl_code_combinations.chart_of_accounts_id
        AND fnd_id_flex_segments.id_flex_code = 'GL#'
        AND fnd_id_flex_segments.application_id = 101
        AND fnd_id_flex_segments.application_column_name = fnd_segment_attribute_values.application_column_name   
    JOIN ebs.fnd_id_flex_segments         fnd_id_flex_segments2 -- profit center
      ON fnd_id_flex_segments2.id_flex_num = gl_code_combinations.chart_of_accounts_id
        AND fnd_id_flex_segments2.id_flex_code = 'GL#'
        AND fnd_id_flex_segments2.application_id = 101
        AND fnd_id_flex_segments2.application_column_name = fnd_segment_attribute_values2.application_column_name 
    JOIN ebs.fnd_id_flex_segments         fnd_id_flex_segments3 -- cost center
      ON fnd_id_flex_segments3.id_flex_num = gl_code_combinations.chart_of_accounts_id
        AND fnd_id_flex_segments3.id_flex_code = 'GL#'
        AND fnd_id_flex_segments3.application_id = 101  
        AND fnd_id_flex_segments3.application_column_name = fnd_segment_attribute_values1.application_column_name
    join ebs.fnd_id_flex_structures
      ON fnd_id_flex_structures.id_flex_num = gl_code_combinations.chart_of_accounts_id
        AND fnd_id_flex_structures.id_flex_code = 'GL#'
        AND fnd_id_flex_structures.application_id = 101
        AND fnd_id_flex_structures.enabled_flag = 'Y'
     """)
.transform(attach_source_column(source = source_name))
.transform(attach_primary_key('glCombinationId, _SOURCE', 'edm.gl_accounts'))
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
  cutoff_value = get_incr_col_max_value(gl_code_combinations,'_MODIFIED')
  update_cutoff_value(cutoff_value, table_name, 'ebs.gl_code_combinations')
  update_run_datetime(run_datetime, table_name, 'ebs.gl_code_combinations')

# COMMAND ----------

# MAGIC %run ../_SHARED/PARTIALS/1.2/footer_s_finance
