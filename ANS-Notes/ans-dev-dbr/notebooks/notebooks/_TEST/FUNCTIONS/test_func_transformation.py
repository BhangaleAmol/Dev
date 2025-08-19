# Databricks notebook source
# MAGIC %run ../../_SHARED/FUNCTIONS/1.3/func_transformation

# COMMAND ----------

import unittest, json
from datetime import date, datetime

# COMMAND ----------

class TestFuncTransformation(unittest.TestCase):

  def test_apply_schema_cast_correct_types(self): 
    source_df = (
      spark
      .createDataFrame([(1, "foo"), (2, "bar")], ["id", "label"])
      .withColumn('empty', f.lit(None))
    )
    
    schema = {'id': 'decimal(22,7)', 'label': 'string', 'foo': 'number'}    
    result_dtypes = source_df.transform(apply_schema(schema)).dtypes
    expected_dtypes = [('id', 'decimal(22,7)'), ('label', 'string'), ('empty', 'string')]
    self.assertTrue(set(result_dtypes) == set(expected_dtypes))
  
  def test_attach_archive_column_has_first_day_of_month_by_default(self):
    source_df = spark.createDataFrame([(1, "foo"), (2, "bar")], ["id", "label"])
    result_df = source_df.transform(attach_archive_column())
    
    result_date = result_df.collect()[0]['_ARCHIVE'].strftime("%Y-%m-%d")
    first_day_of_month =  date.today().replace(day=1).strftime("%Y-%m-%d")
    self.assertEqual(result_date, first_day_of_month)
    
  def test_attach_archive_column_has_current_day_for_daily(self):  
    source_df = spark.createDataFrame([(1, "foo"), (2, "bar")], ["id", "label"])
    result_df = source_df.transform(attach_archive_column(archive_type = "d"))
    
    result_date = result_df.collect()[0]['_ARCHIVE'].strftime("%Y-%m-%d")
    current_date =  date.today().strftime("%Y-%m-%d")
    self.assertEqual(result_date, current_date)
    
  def test_attach_deleted_flag_column_exists(self):
    source_df = spark.createDataFrame([(1, "foo"), (2, "bar")], ["id", "label"])
    result_df = source_df.transform(attach_deleted_flag())
    self.assertTrue("_DELETED" in result_df.columns)
    
  def test_attach_modified_date_correct_date(self):
    source_df = spark.createDataFrame([(1, "foo"), (2, "bar")], ["id", "label"])
    result_df = source_df.transform(attach_modified_date())
    
    result_date = result_df.collect()[0]['_MODIFIED'].strftime("%Y-%m-%d")
    current_date =  date.today().strftime("%Y-%m-%d")
    self.assertEqual(result_date, current_date)
    
  def test_attach_partition_column_correct_date(self):
    source_df = (
      spark
      .createDataFrame([(1, "foo"), (2, "bar")], ["id", "label"])
      .withColumn('current_date', f.lit(f.current_date()))
    ) 
    result_df = source_df.transform(attach_partition_column('current_date'))
    
    result_date = result_df.collect()[0]['_PART'].strftime("%Y-%m-%d")
    current_date =  date.today().replace(day=1).strftime("%Y-%m-%d")
    self.assertEqual(result_date, current_date)
    
  def test_attach_partition_column_for_other_formats(self):
    source_df = (
      spark.range(1).drop("id")  
      .withColumn('default_date', f.lit('2021-12-02'))
      .withColumn('datetime', f.lit('2021-12-02T12:00:00.000Z'))
      .withColumn('us_date', f.lit('12/02/2021'))
      .withColumn('no_date', f.lit(None))
      .withColumn('date_type', f.lit('2021-12-02').cast('date'))
      .withColumn('datetime_type', f.lit('2021-12-02T12:00:00.000Z').cast('timestamp'))
    )

    result_df = (
      source_df
      .transform(attach_partition_column('default_date', name = "default_date"))
      .transform(attach_partition_column('datetime', name = "datetime"))
      .transform(attach_partition_column('us_date', src_pattern = 'MM/dd/yyyy', name = "us_date"))
      .transform(attach_partition_column('no_date', name = "no_date"))
      .transform(attach_partition_column('date_type', name = "date_type"))
      .transform(attach_partition_column('datetime_type', name = "datetime_type"))
    )

    schema = StructType([
      StructField("default_date", DateType(), True),
      StructField("datetime", DateType(), True),
      StructField("us_date", DateType(), True),
      StructField("no_date", DateType(), True),
      StructField("date_type", DateType(), True),
      StructField("datetime_type", DateType(), True)
    ])

    date = datetime.strptime('2021-12-01', "%Y-%m-%d").date()
    expected_df = spark.createDataFrame([(date, date, date, None, date, date)], schema)
    self.assertEqual(sorted(result_df.collect()), sorted(expected_df.collect()))
    
  def test_attach_source_column_correct_value(self):
    source_df = spark.createDataFrame([(1, "foo"), (2, "bar")], ["id", "label"])
    result_df = source_df.transform(attach_source_column('test'))
    result = result_df.collect()[0]['_SOURCE']
    self.assertEqual(result, 'test')
    
  def test_attach_subset_column_correct_value(self):
    source_df = spark.createDataFrame([(1, "foo"), (2, "bar")], ["id", "label"])
    result_df = source_df.transform(attach_subset_column('test'))
    result = result_df.collect()[0]['_SUBSET']
    self.assertEqual(result, 'test')
    
  def test_attach_surrogate_key(self):
    source_df = spark.createDataFrame([(1, "foo")], ["id", "label"])
    result_df = source_df.transform(attach_surrogate_key('id,label'))
    result = result_df.collect()[0]['_ID']
    self.assertEqual(result, '9f4ab64bab06e9640b65afe4d1926d13d571e911e19003bb58a96474516bf0f9')
    
  def test_attach_surrogate_key_with_null(self):
    source_df = spark.createDataFrame([(1, None), (2, "bar")], ["id", "label"])
    result_df = source_df.transform(attach_surrogate_key('id,label', name = '_TEST'))
    result = result_df.collect()[0]['_TEST']
    self.assertEqual(result, '0')
    
  def test_attach_unknown_record(self):
    schema = StructType([
      StructField("_ID", StringType(), True),
      StructField("FK_ID", StringType(), True),
      StructField("_SOURCE", StringType(), True),
      StructField("_DELETED", BooleanType(), True),
      StructField("_MODIFIED", TimestampType(), True),
      StructField("label", StringType(), True),
      StructField("value", IntegerType(), True)
    ])

    current_datetime =  datetime.today()

    source_df = spark.createDataFrame([("1", "2", "SRC", False, current_datetime, "foo", 1)], schema)
    result_df = attach_unknown_record(source_df)
    result_df = result_df.withColumn('_MODIFIED', f.to_date(f.col('_MODIFIED'))).limit(1)
    expected_df = spark.createDataFrame([("0", "0", "DEFAULT", False, current_datetime, "Unknown", None)], schema)
    expected_df = expected_df.withColumn('_MODIFIED', f.to_date(f.col('_MODIFIED')))

    self.assertEqual(sorted(expected_df.collect()), sorted(result_df.collect()))
  
  def test_cast_all_null_types(self):
    source_df = (
      spark.createDataFrame([(1,), (2,)], ["id"])
      .withColumn('null_type', f.lit(None))
    )    
    result_df = source_df.transform(cast_all_null_types())
    self.assertEqual(result_df.dtypes[1][1], 'string')
  
  def test_cast_data_type(self):
    source_df = (
      spark.createDataFrame([(1,), (2,)], ["id"])
      .withColumn('null_type', f.lit(None))
    )    
    result_df = source_df.transform(cast_data_type(columns = ['id', 'null_type']))    
    self.assertTrue(result_df.dtypes[0][1] == 'string' and result_df.dtypes[1][1] == 'string')
  
  def test_change_to_title_case(self):
    source_df = spark.createDataFrame([
      (1, "with space"), (2, "with_underscore"), (3, "ALL_CAPS"),
      (4, "cammelCase"), (5, "with/separator"), (6, None),
    ], ["id", "label"])

    result_df = source_df.transform(change_to_title_case(["label"]))

    expected_df = spark.createDataFrame([
      (1, "With Space"), (2, "With_Underscore"), (3, "All_Caps"),
      (4, "Cammelcase"), (5, "With/Separator"), (6, None),
    ], ["id", "label"])

    self.assertEqual(sorted(result_df.collect()), sorted(expected_df.collect()))   
  
  def test_check_to_upper_case(self):
    columns = ["id", "label1", "label2"]
    source_df = spark.createDataFrame([
      (1, "label1", "label2"), 
      (2, None, "label2")], columns)    
    result_df = source_df.transform(change_to_upper_case())
    expected_df = spark.createDataFrame([
      ('1', 'LABEL1', 'LABEL2'),
      ('2', None, 'LABEL2')], columns)
    self.assertEqual(sorted(result_df.collect()), sorted(expected_df.collect()))
  
  def test_clean_multiple_spaces(self):
    columns = ["id", "label1", "label2"]
    source_df = spark.createDataFrame([(1, "label1  ", "  label  2  ")], columns)
    result_df = source_df.transform(clean_multiple_spaces())
    expected_df = spark.createDataFrame([('1', 'label1', 'label 2')], columns)
    self.assertEqual(sorted(result_df.collect()), sorted(expected_df.collect()))
  
  def test_clean_unknown_values(self):
    schema = StructType([
      StructField("id", IntegerType(), True),
      StructField("label1", StringType(), True),
      StructField("label2", StringType(), True),
      StructField("label3", StringType(), True)
    ])

    source_df = spark.createDataFrame([(1, "-", "n/a", "not provided")], schema)
    result_df = source_df.transform(clean_unknown_values())
    expected_df = spark.createDataFrame([(1, None, None, None)], schema)
    self.assertEqual(sorted(result_df.collect()), sorted(expected_df.collect()))
  
  def test_convert_empty_string_to_null(self):
    schema = StructType([
      StructField("id", StringType(), True),
      StructField("label1", StringType(), True),
      StructField("label2", StringType(), True)
    ])

    source_df = spark.createDataFrame([(1, "label1", "")], schema)
    result_df = source_df.transform(convert_empty_string_to_null())
    expected_df = spark.createDataFrame([(1, 'label1', None)], schema)

    self.assertEqual(sorted(result_df.collect()), sorted(expected_df.collect()))
  
  def test_convert_epoch_to_timestamp(self):
    source_df = spark.createDataFrame([(1, 1628497235000)], ["id", "epoch"])
    result_df = source_df.transform(convert_epoch_to_timestamp(['epoch']))
    result = result_df.collect()[0]['epoch']
    excepted_resut = datetime.strptime('2021-08-09 08:20:35', "%Y-%m-%d %H:%M:%S")
    self.assertEqual(excepted_resut, result)
  
  def test_convert_nan_to_null(self):
    schema = StructType([
      StructField("id", IntegerType(), True),
      StructField("label1", StringType(), True),
    ])

    source_df = spark.createDataFrame([(1, "NaN",)], schema)
    result_df = source_df.transform(convert_nan_to_null())
    expected_df = spark.createDataFrame([(1, None)], schema)
    self.assertEqual(sorted(result_df.collect()), sorted(expected_df.collect()))  
  
  def test_convert_null_to_unknown(self):
    schema = StructType([
      StructField("id", StringType(), True),
      StructField("label1", StringType(), True),
    ])

    source_df = spark.createDataFrame([('1', None,)], schema)
    result_df = source_df.transform(convert_null_to_unknown())
    expected_df = spark.createDataFrame([('1', 'Unknown')], schema)
    self.assertEqual(sorted(result_df.collect()), sorted(expected_df.collect()))
  
  def test_convet_null_to_zero(self):    
    source_df = spark.createDataFrame([(1, None),(None, 'bar')], ["id", "label"])
    result_df = source_df.transform(convert_null_to_zero())
    expected_df = spark.createDataFrame([('1', '0'), ('0', 'bar')], ["id", "label"])
    self.assertEqual(sorted(result_df.collect()), sorted(expected_df.collect()))
  
  def test_convert_null_string_to_null(self): 
    source_df = spark.createDataFrame([(1, 'null'), (2, 'test')], ["id", "label"])
    result_df = source_df.transform(convert_null_string_to_null(['label']))
    expected_df = spark.createDataFrame([(1, None), (2, 'test')], ["id", "label"])
    self.assertEqual(sorted(expected_df.collect()), sorted(result_df.collect()))
  
  def test_drop_duplicate_columns(self): 
    source_df = spark.createDataFrame([("foo", "foo"), ("bar", "bar")], ["id", "id"])
    result_df = source_df.transform(drop_duplicate_columns)
    expected_df = spark.createDataFrame([("foo",), ("bar",)], ["id"])
    self.assertEqual(sorted(expected_df.collect()), sorted(result_df.collect()))
 
  def test_fix_cell_values(self):
    columns = ["id", "label"]
    source_df = spark.createDataFrame([('1  ', 'n/a'),('', 'bar  ')], columns)
    result_df = source_df.transform(fix_cell_values())
    expected_df = spark.createDataFrame([('1', None), (None, 'bar')], columns)
    self.assertEqual(result_df.collect(), expected_df.collect())

  def test_fix_column_names(self):
    source_df = spark.createDataFrame([(1, "foo")], ["id!@#", "label_test space"])
    result_columns = source_df.transform(fix_column_names).columns
    expected_columns = ['id', 'label_testspace']
    self.assertEqual(sorted(result_columns), sorted(expected_columns))
    
  def test_fix_dates(self):
    source_df = spark.createDataFrame([
      (1, '1800-01-01'),(2, '1902-01-01')], ["id", "label"])
    result_df = source_df.transform(fix_dates(['label']))
    expected_df = spark.createDataFrame([
      (1, None), (2, '1902-01-01')], ["id", "label"])
    self.assertEqual(sorted(result_df.collect()), sorted(expected_df.collect()))
  
  def test_parse_date(self):
    source_df = spark.createDataFrame([
      (1, '2020-01-20'),
      (2, '01/20/2020'),
      (3, None),
      (4, 'some string')], ["id", "label"])
    
    formats = ['yyyy-MM-dd', 'MM/dd/yyyy']
    result_df = source_df.transform(parse_date(['label'], expected_format = formats))
    
    expected_df = spark.createDataFrame([
      (1, '2020-01-20'), 
      (2, '2020-01-20'),
      (3, None),
      (4, 'some string')], ["id", "label"])
    self.assertEqual(sorted(result_df.collect()), sorted(expected_df.collect()))
  
  def test_parse_timestamp(self):
    columns = ["id", "label"]
    source_df = spark.createDataFrame([
      (1, '2020-01-20 10:20:30'),
      (2, '01/20/2020.10:20:30'),
      (3, None),
      (4, 'some string')], columns)
    
    formats = ['yyyy-MM-dd HH:mm:ss', 'MM/dd/yyyy.HH:mm:ss']
    result_df = source_df.transform(parse_timestamp(['label'], expected_format = formats))
    expected_df = spark.createDataFrame([
      (1, '2020-01-20 10:20:30'), 
      (2, '2020-01-20 10:20:30'),
      (3, None),
      (4, 'some string')], columns)

    self.assertEqual(sorted(result_df.collect()), sorted(expected_df.collect()))
  
  def test_trim_all_values(self):
    source_df = spark.createDataFrame([("foo ", " foo"), ("bar  ", "  bar")], ["id", "label"])
    result_df = source_df.transform(trim_all_values)
    expected_df = spark.createDataFrame([("foo", "foo"), ("bar", "bar")], ["id", "label"])
    self.assertEqual(sorted(result_df.collect()), sorted(expected_df.collect()))
    
  def test_days_between_udf(self):
    source_df = spark.createDataFrame([
      (1, '2020-01-20', '2020-01-22'),
      (2, '2020-01-22', '2020-01-20'),
      (3, None, '2020-01-22')], ["id", "date1", "date2"])

    result_df = source_df.withColumn('days_between', days_between_udf(
      source_df['date1'], source_df['date2']))

    expected_df = spark.createDataFrame([
      (1, '2020-01-20', '2020-01-22', 2),
      (2, '2020-01-22', '2020-01-20', -2),
      (3, None, '2020-01-22', 0)], ["id", "date1", "date2", "days_between"])

    self.assertEqual(sorted(result_df.collect()), sorted(expected_df.collect()))

# COMMAND ----------

suite = unittest.TestSuite()
suite.addTests(unittest.makeSuite(TestFuncTransformation))
result = unittest.TextTestRunner(verbosity=2).run(suite)

# COMMAND ----------

failures = []
for failure in result.failures:
  failures.append(str(failure[0]))
  
for errors in result.errors:
  failures.append(str(errors[0]))

# COMMAND ----------

dbutils.notebook.exit(json.dumps({
  "successfull": result.wasSuccessful(),
  "failures": failures
}))
