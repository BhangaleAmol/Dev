# Databricks notebook source
# EXPORT TABLE SCHEMA
table_schema = export_table_schema(table_name)
dbutils.fs.put(metadata_file_path, table_schema, True)
print(f'table_schema exported')
print(metadata_file_path)
