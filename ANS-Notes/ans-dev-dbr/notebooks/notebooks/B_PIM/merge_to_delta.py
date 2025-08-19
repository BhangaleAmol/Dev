# Databricks notebook source
# MAGIC %run ../_SHARED/FUNCTIONS/1.2/bootstrap

# COMMAND ----------

# MAGIC %run ../_SHARED/PARTIALS/1.2/header_bronze_notebook_pim

# COMMAND ----------

# append_only = False # [bool]
# database_name = "PIM" # [str]
# handle_delete = False # [bool]
# incremental = False # [bool]
# incremental_column = None # [NoneType]
# key_columns = "MdmId" # [str]
# overwrite = True # [bool]
# partition_column = None # [NoneType]
# prune_days = 30 # [int]
# sampling = False # [bool]
# sheet_id = None # [NoneType]
# schema_name = 'pim'
# source_folder = "/datalake/PIM/raw_data/delta_data" # [str]
# table_name = "products" # [str]
# target_container = "datalake" # [str]
# target_folder = "/datalake/PIM/raw_data/full_data" # [str]
# target_storage = "edmans{env}data001" # [str]
# test_run = False # [bool]

# COMMAND ----------

# VALIDATE SOURCE DETAILS
if source_folder is None or table_name is None:
  raise Exception("Source data details are missing")
  
# VALIDATE TARGET DETAILS
if target_folder is None or database_name is None or table_name is None:
  raise Exception("Target details are missing")
  
# VALIDATE OVERWRITE
if incremental and overwrite:
  raise Exception("INCREMENTAL with OVERWRITE not allowed")
  

# COMMAND ----------

# SETUP VARIABLES
file_name = get_file_name(None, table_name).lower()
table_name = get_table_name(database_name, None, table_name)
source_file_path = get_file_path(source_folder, f'{file_name}.json', target_container, target_storage)

print('table_name: ' + table_name)
print('file_name: ' + file_name)
print('source_file_path: ' + source_file_path)

sourceName = database_name
print('source_name: ' + sourceName)

# COMMAND ----------

import pyspark.sql.types as t

# COMMAND ----------

key_columns_list = key_columns
if key_columns is not None:
  key_columns = key_columns.replace(' ', '').split(',')
  if not isinstance(key_columns, list):
    key_columns = [key_columns]

# COMMAND ----------

def drop_duplicate_columns(df):
  columns_renamed = [] 
  seen = set()
  for c in df.columns:
      columns_renamed.append('{}_dup'.format(c) if c in seen else c)
      seen.add(c)

  columns = [c for c in columns_renamed if not c.endswith('_dup')]
  return df.toDF(*columns_renamed).select(*columns)

# COMMAND ----------

def rename_duplicate_columns(df):
    columns = df.columns
    duplicate_column_indices = list(set([columns.index(col) for col in columns if columns.count(col) == 2]))
    for index in duplicate_column_indices:
        columns[index] = columns[index]+'2'
    df = df.toDF(*columns)
    return df

# COMMAND ----------

spark.conf.set("spark.sql.caseSensitive", True)
df1 = spark.read.format("json")\
                .option("multiLine", "true")\
                .load(source_file_path)

spark.conf.set("spark.sql.caseSensitive", False)

# COMMAND ----------

# df1.printSchema()
# df1.show()

# COMMAND ----------

df2 =(
      df1.select('TotalNumberOfElements', 'TotalPages', f.explode('Products'))
         .select('TotalNumberOfElements', 'TotalPages', 'col.*')
)


df2 = df2.filter("Not(MdmId is null) AND MdmId <> ''")
# display(df2)

# COMMAND ----------

# print(df2.count())
# df2.createOrReplaceTempView('df2')

# COMMAND ----------

spark.conf.set("spark.sql.caseSensitive", True)
main3 = (
  df2
  .withColumn('_DELETED', f.lit(False))
  .withColumn('_MODIFIED', f.current_timestamp())
  .withColumn("AntiStatic",f.when(f.col("AntiStatic").isNull(), f.col("Antistatic")).otherwise(f.col("AntiStatic")))
  .withColumn("BFE",f.when(f.col("BFE").isNull(), f.col("Bfe")).otherwise(f.col("BFE")))
  .drop("Antistatic")
  .drop("Bfe")
)
spark.conf.set("spark.sql.caseSensitive", False)

main3.createOrReplaceTempView('main3')

# COMMAND ----------

# main3.display()
# main3.createOrReplaceTempView('main3')

# COMMAND ----------

att_name = 'Value'
att_name1 = 'url'
for column in main3.columns:
    if isinstance(main3.schema[column].dataType, StructType) :
      for field in main3.schema[column].dataType.fields:
        if field.name == att_name:
           main3 = (
                    main3
                       .withColumn(column, f.col(f'{column}.{att_name}').cast("string"))
                       .withColumn(column, f.regexp_replace(column,'[;)(*&^%$#@!©<>,’”/"]',""))\
                       .withColumn(column, f.regexp_replace(column,"[\\r|\\n]",""))\
                       .withColumn(column, f.regexp_replace(f.trim(f.col(column)), '\\s+', ' '))
                   )
        if field.name.lower() == att_name1:
           main3 = (
                    main3
                       .withColumn(column, f.col(f'{column}.{att_name1}').cast("string"))
                   )
        if column == "FoodDeclarationCategory" :
          main3 = (
                   main3
                    .withColumn(column, f.col(column).cast("string"))
                 )
    if isinstance(main3.schema[column].dataType,  t.ArrayType):
      main3 = (
                 main3
                  .withColumn(column, f.to_json(column))
               )


# COMMAND ----------

# PARTITION DATA
if partition_column is not None:
  main3 = main3.transform(attach_partition_column(partition_column))
  partition_field = '_PART'
else:
  main3 = main3
  partition_field = None

# COMMAND ----------

main_f = (
  main3
  .withColumn('_DELETED', f.lit(False))
  .withColumn('_MODIFIED', f.current_timestamp())
  # BASIC CLEANUP
  .transform(fix_column_names)
  .transform(trim_all_values)
  .transform(drop_duplicate_columns)
  .transform(sort_columns)
)

# COMMAND ----------

# show_duplicate_rows(main_f, key_columns)

# COMMAND ----------

# DUPLICATES NOTIFICATION
if key_columns is not None:
  duplicates = get_duplicate_rows(main_f, key_columns)
  duplicates.cache()

  if not duplicates.rdd.isEmpty():
    notebook_data = {
      'source_name': sourceName,
      'notebook_name': NOTEBOOK_NAME,
      'notebook_path': NOTEBOOK_PATH,
      'target_name': table_name,
      'duplicates_count': duplicates.count(),
      'duplicates_sample': duplicates.select(key_columns).limit(50)
    }
    send_mail_duplicate_records_found(notebook_data)
    main_f = main_f.dropDuplicates(key_columns)
  
  duplicates.unpersist()

# COMMAND ----------

# VALIDATE DATA
if key_columns is not None:
  valid_count_rows(main_f, key_columns)
  
# main_f.createOrReplaceTempView('main_f')

# COMMAND ----------

options = {
  'append_only': append_only,
  'incremental': incremental,
  'incremental_column': incremental_column,
  'overwrite': overwrite,
  'partition_column': partition_field,
  'sampling': sampling,
  'target_container': target_container,
  'target_storage': target_storage
}

merge_raw_to_delta(main_f, table_name, key_columns, target_folder, options)

# COMMAND ----------

# RETURN MAX VALUE
max_value = get_incr_col_max_value(df2)
dbutils.notebook.exit(json.dumps({"max_value": max_value}))
