# Databricks notebook source
# MAGIC %run ../_SHARED/FUNCTIONS/1.3/bootstrap

# COMMAND ----------

# INPUT PARAMETERS
sampling = get_input_param('sampling', 'bool', False)
source_folder = get_input_param('source_folder', default_value = '/datalake/CTI/raw_data/delta_data')
table_name = get_input_param('table_name', default_value = 'CSRS')
target_folder = get_input_param('target_folder', default_value = '/datalake/CTI/raw_data/delta_data_col')

# COMMAND ----------

# SETUP VARIABLES
file_name = get_file_name(table_name, 'par')
file_path_par = get_file_path(source_folder, file_name)

file_name = get_file_name(table_name, 'json')
file_path_json = get_file_path(source_folder, file_name)

file_name = get_file_name(table_name, 'par')
target_file_path = get_file_path(target_folder, file_name)

print('file_path_par: ' + file_path_par)
print('file_path_json: ' + file_path_json)
print('target_file_path: ' + target_file_path)

# COMMAND ----------

main_df = spark.read.format('parquet').load(file_path_par)
json_df = spark.read.json(file_path_json, multiLine=True)

# COMMAND ----------

# SAMPLING
if sampling:
  main_df = main_df.limit(10)

# COMMAND ----------

# EXTRACT COLUMN NAMES
agg_df = json_df.select("query.aggregations")
column_rows = (
  agg_df.withColumn("aggregations", f.explode(f.col("aggregations")))
    .sort("aggregations.id")
    .select("aggregations.computeColumnName")
    .collect()
)
column_list = [row['computeColumnName'] for row in column_rows]
columns = ["ID", *column_list]
print(columns)

# COMMAND ----------

# RENAME COLUMNS
main_df = main_df.toDF(*columns)
main_f = drop_duplicate_columns(main_df)
display(main_f)

# COMMAND ----------

main_f.write.format('parquet').save(target_file_path, mode="overwrite")

# COMMAND ----------


