# Databricks notebook source
# MAGIC %run ../_SHARED/FUNCTIONS/1.3/bootstrap

# COMMAND ----------

# MAGIC %run ../_SCHEMA/g_na_tm.pos_location

# COMMAND ----------

# INPUT
database_name = get_input_param('database_name', default_value = 'g_na_tm')
incremental = get_input_param('incremental', 'bool', default_value = False)
key_columns = get_input_param('key_columns', 'list', default_value = ['location_id'])
overwrite = get_input_param('overwrite', 'bool', default_value = True)
prune_days = get_input_param('prune_days', 'int', default_value = 30)
sampling = get_input_param('sampling', 'bool', default_value = False)
table_name = get_input_param('table_name', default_value = 'pos_location')
target_folder = get_input_param('target_folder', default_value = '/datalake_gold/na_tm/full_data')

# COMMAND ----------

# VALIDATE INPUT
if incremental and key_columns is None:
  raise Exception("INCREMENTAL & NO KEY COLUMNS")

if overwrite and sampling:
  raise Exception("OVERWRITE & SAMPLING")

# COMMAND ----------

# VARIABLES
target_table = get_table_name(database_name, table_name)

# COMMAND ----------

# EXTRACT
source_table = 'g_na_tm.wc_qv_pos_a'
if incremental:
  cutoff_value = get_cutoff_value(target_table, source_table, prune_days)  
  wc_qv_pos_a = load_incremental_dataset(source_table, '_MODIFIED', cutoff_value)
else:
  wc_qv_pos_a = load_full_dataset(source_table)
  
wc_qv_pos_a.createOrReplaceTempView('wc_qv_pos_a')
wc_qv_pos_a.display()

# COMMAND ----------

# SAMPLING
if sampling:
  wc_qv_pos_a = wc_qv_pos_a.limit(10)
  wc_qv_pos_a.createOrReplaceTempView('wc_qv_pos_a')

# COMMAND ----------

main_df = spark.sql("""

SELECT 
DISTINCT
CREATEDON,
upper(state) state,
upper(city) city,
upper(postalcode) postalcode,
upper(location_id) location_id
from (


 select
  distinct 
  current_date() CREATEDON,
  nvl(
    nvl(Final_Lookup_canada.state, wc_qv_pos_a.state),
    'unknown'
  ) state,
  nvl(nvl(primary_city, END_USER_CITY), 'unknown') city,
  nvl(
    nvl(
      CASE
        WHEN wc_qv_pos_a.SUBREGION = 'US' THEN SUBSTR(
          regexp_replace(wc_qv_pos_a.postalcode, ' ', ''),
          1,
          5
        )
        ELSE SUBSTR(
          regexp_replace(wc_qv_pos_a.postalcode, ' ', ''),
          1,
          6
        )
      END,
      Final_Lookup_canada.Postalcode
    ),
    'unknown'
  ) postalcode,
  concat(
    nvl(
      nvl(Final_Lookup_canada.state, wc_qv_pos_a.state),
      'unknown'
    ),
    '-',
    nvl(nvl(primary_city, END_USER_CITY), 'unknown'),
    '-',
    nvl(
      nvl(
        CASE
          WHEN wc_qv_pos_a.SUBREGION = 'US' THEN SUBSTR(
            regexp_replace(wc_qv_pos_a.postalcode, ' ', ''),
            1,
            5
          )
          ELSE SUBSTR(
            regexp_replace(wc_qv_pos_a.postalcode, ' ', ''),
            1,
            6
          )
        END,
        Final_Lookup_canada.Postalcode
      ),
      'unknown'
    )
  ) location_id
from
  g_na_tm.wc_qv_pos_a
  LEFT JOIN (select distinct * from amazusftp1.Final_Lookup_canada) Final_Lookup_canada ON CASE
    WHEN wc_qv_pos_a.SUBREGION = 'US' THEN SUBSTR(
      regexp_replace(wc_qv_pos_a.postalcode, ' ', ''),
      1,
      5
    )
    ELSE SUBSTR(
      regexp_replace(wc_qv_pos_a.postalcode, ' ', ''),
      1,
      6
    )
  END = regexp_replace(Final_Lookup_canada.Postalcode, ' ', '')
union
select
  distinct 
  current_date() CREATEDON,
  nvl(
    nvl(Final_Lookup_canada.state, wc_qv_pos_dis_a.state),
    'unknown'
  ) state,
  nvl(nvl(primary_city, END_USER_CITY), 'unknown') city,
  nvl(
    nvl(
      CASE
        WHEN wc_qv_pos_dis_a.SUBREGION = 'US' THEN SUBSTR(
          regexp_replace(wc_qv_pos_dis_a.postalcode, ' ', ''),
          1,
          5
        )
        ELSE SUBSTR(
          regexp_replace(wc_qv_pos_dis_a.postalcode, ' ', ''),
          1,
          6
        )
      END,
      Final_Lookup_canada.Postalcode
    ),
    'unknown'
  ) postalcode,
  concat(
    nvl(
      nvl(Final_Lookup_canada.state, wc_qv_pos_dis_a.state),
      'unknown'
    ),
    '-',
    nvl(nvl(primary_city, END_USER_CITY), 'unknown'),
    '-',
    nvl(
      nvl(
        CASE
          WHEN wc_qv_pos_dis_a.SUBREGION = 'US' THEN SUBSTR(
            regexp_replace(wc_qv_pos_dis_a.postalcode, ' ', ''),
            1,
            5
          )
          ELSE SUBSTR(
            regexp_replace(wc_qv_pos_dis_a.postalcode, ' ', ''),
            1,
            6
          )
        END,
        Final_Lookup_canada.Postalcode
      ),
      'unknown'
    )
  ) location_id
from
  g_na_tm.wc_qv_pos_dis_a
  LEFT JOIN (select distinct * from amazusftp1.Final_Lookup_canada) Final_Lookup_canada ON CASE
    WHEN wc_qv_pos_dis_a.SUBREGION = 'US' THEN SUBSTR(
      regexp_replace(wc_qv_pos_dis_a.postalcode, ' ', ''),
      1,
      5
    )
    ELSE SUBSTR(
      regexp_replace(wc_qv_pos_dis_a.postalcode, ' ', ''),
      1,
      6
    )
  END = regexp_replace(Final_Lookup_canada.Postalcode, ' ', '')
union
select
  distinct 
  current_date() CREATEDON,
  nvl(
    nvl(
      Final_Lookup_canada.state,
      wc_qv_pos_manadj_a.state
    ),
    'unknown'
  ) state,
  nvl(nvl(primary_city, END_USER_CITY), 'unknown') city,
  nvl(
    nvl(
      CASE
        WHEN wc_qv_pos_manadj_a.SUBREGION = 'US' THEN SUBSTR(
          regexp_replace(wc_qv_pos_manadj_a.postalcode, ' ', ''),
          1,
          5
        )
        ELSE SUBSTR(
          regexp_replace(wc_qv_pos_manadj_a.postalcode, ' ', ''),
          1,
          6
        )
      END,
      Final_Lookup_canada.Postalcode
    ),
    'unknown'
  ) postalcode,
  concat(
    nvl(
      nvl(
        Final_Lookup_canada.state,
        wc_qv_pos_manadj_a.state
      ),
      'unknown'
    ),
    '-',
    nvl(nvl(primary_city, END_USER_CITY), 'unknown'),
    '-',
    nvl(
      nvl(
        CASE
          WHEN wc_qv_pos_manadj_a.SUBREGION = 'US' THEN SUBSTR(
            regexp_replace(wc_qv_pos_manadj_a.postalcode, ' ', ''),
            1,
            5
          )
          ELSE SUBSTR(
            regexp_replace(wc_qv_pos_manadj_a.postalcode, ' ', ''),
            1,
            6
          )
        END,
        Final_Lookup_canada.Postalcode
      ),
      'unknown'
    )
  ) location_id
from
  g_na_tm.wc_qv_pos_manadj_a
  LEFT JOIN (select distinct * from amazusftp1.Final_Lookup_canada) Final_Lookup_canada ON CASE
    WHEN wc_qv_pos_manadj_a.SUBREGION = 'US' THEN SUBSTR(
      regexp_replace(wc_qv_pos_manadj_a.postalcode, ' ', ''),
      1,
      5
    )
    ELSE SUBSTR(
      regexp_replace(wc_qv_pos_manadj_a.postalcode, ' ', ''),
      1,
      6
    )
  END = regexp_replace(Final_Lookup_canada.Postalcode, ' ', '')
  )
""")

main_df.cache()
main_df.display()

# COMMAND ----------

# TRANSFORM DATA
main_f = (
   main_df
   .transform(attach_partition_column("CREATEDON"))
   .transform(attach_modified_date())
   .transform(attach_deleted_flag())
   .transform(apply_schema(schema))
)
main_f.display()

# COMMAND ----------

# LOAD
options = {'overwrite': overwrite, 'partition_column': '_PART'}
register_hive_table(main_f, target_table, target_folder, options = options)
merge_into_table(main_f, target_table, key_columns, options = {'auto_merge': True})
