# Databricks notebook source
# MAGIC %run ../_SHARED/FUNCTIONS/1.3/bootstrap

# COMMAND ----------

file_name = get_input_param('file_name')
key_columns = get_input_param('key_columns')
source_folder = get_input_param('source_folder')
target_folder = get_input_param('target_folder')

# COMMAND ----------

key_columns = key_columns.replace(' ', '').split(',')
if not isinstance(key_columns, list):
  key_columns = [key_columns]

# COMMAND ----------

# LIST FILES
source_file = []
source_file_prefix = file_name.replace('*.csv', '')

files = [f[1] for f in dbutils.fs.ls(f'/mnt/datalake/{source_folder}') if f[1].startswith(source_file_prefix)] 
for file in files:
  source_file.append(get_file_path(source_folder, file))

print("\n".join(source_file))  
target_file = get_file_path(target_folder, file_name.replace('*', 'All'))
print(target_file)

# COMMAND ----------

def read_data_file(source_file):
  return (spark.read
    .format('csv')
    .option("header", True)
    .option("escape", "\"")
    .option("quote", "\"")
    .option("multiline", "true")
    .option("delimiter","|")
    .load(source_file)
    .distinct()        
  )

def add_missing_columns(df, col_list):  
  for col in col_list:  
    if not col in df.columns:
      df = df.withColumn(col, f.lit(None))
  return df

# COMMAND ----------

# READ DATA
files_with_25k = []
source_dict = {}
for k, v in enumerate(source_file):
  source_dict[k] = read_data_file(v)
  if source_dict[k].count() >= 25000:
    files_with_25k.append(v)

# COMMAND ----------

# SEND 25k NOTIFICATION
if files_with_25k:
  send_mail_pdh_25k_records_found(files_with_25k)

# COMMAND ----------

# GET ALL COLUMNS
col_list = []
for k, v in source_dict.items():
  col_list = list(set(col_list + v.columns))
    
print(col_list)

# COMMAND ----------

# ADD COLUMNS
for k, v in source_dict.items():
  source_dict[k] = add_missing_columns(v, col_list).select(col_list)

# COMMAND ----------

# JOIN DATA
main = spark.range(1).drop('id')
for col in col_list:
  main = main.withColumn(col, f.lit(None))
  
for k, v in source_dict.items():
  main = main.union(source_dict[k])

# COMMAND ----------

if 'ITEM_NAME' in main.columns:
  main = main.withColumn('ITEM', f.col('ITEM_NAME'))

# COMMAND ----------

# TRANSFORM DATA
main_2 = (
  main
  .filter("ITEM != 'EMPTY'")
  .transform(clean_unknown_values(values = ['EMPTY', 'NO DATA']))
  .transform(fix_cell_val())
  .na.drop("all")
  .na.drop(how = 'any', subset = key_columns)
  .transform(replace_character(old_char = '“', new_char = '"'))
  .distinct()
)

main_2.createOrReplaceTempView('main_2')
main_2.cache()

# COMMAND ----------

# SEND DUPLICATES NOTIFICATION
duplicates = get_duplicate_rows(main_2, key_columns)
duplicates.cache()

if not duplicates.rdd.isEmpty():
  notebook_data = {
    'source_name': 'PDH',
    'notebook_name': NOTEBOOK_NAME,
    'notebook_path': NOTEBOOK_PATH,
    'target_name': file_name,
    'duplicates_count': duplicates.count(),
    'duplicates_sample': duplicates.select(key_columns).limit(50)
  }
  send_mail_duplicate_records_found(notebook_data)

duplicates.unpersist()

# COMMAND ----------

# DROP DUPLICATES
main_f = main_2.dropDuplicates(key_columns)

# COMMAND ----------

# WRITE DATA
main_f.write.format('csv').mode('overwrite').save(target_file, header = 'true')
