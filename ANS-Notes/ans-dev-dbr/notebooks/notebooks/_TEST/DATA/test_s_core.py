# Databricks notebook source
# MAGIC %run ./bootstrap

# COMMAND ----------

# MAGIC %run ../../_SCHEMA/s_core

# COMMAND ----------

table_details = get_s_core_schema()

# COMMAND ----------

database_name = 's_core'
if database_name not in get_databases():
  dbutils.notebook.exit()  

# COMMAND ----------

table_names = get_tables(database_name)
table_names = [t for t in table_names if t not in ['s_core.public_holidays']]

# COMMAND ----------

class TestCore(unittest.TestCase):
  
  @parameterized.expand(table_names, skip_on_empty=True)
  def test_table_not_broken(self, table_name):
    spark.table(table_name).sample(False, 0.1, seed=0).limit(1)
    
  @parameterized.expand(table_names, skip_on_empty=True)
  def test_not_all_deleted_true(self, table_name):
    result = spark.sql(f"""
      SELECT COUNT(*) AS RESULT
      FROM {table_name}
      WHERE _DELETED IS FALSE
    """).collect()[0]['RESULT']
    self.assertTrue(result > 0)
  
  @parameterized.expand(table_names, skip_on_empty=True)
  def test_pk_is_unique(self, table_name):
    df = spark.table(table_name).select('_ID')
    df.cache()
    self.assertEqual(df.count(), df.distinct().count())
  
  @parameterized.expand(
    get_test_has_no_nk_duplicates_args(table_details), 
    skip_on_empty=True
  )
  def test_has_no_nk_duplicates(self, name, table_name, keys, source_name):
    
    df = spark.table(table_name) \
      .filter(f"_SOURCE = '{source_name}'") \
      .select(keys)
      
    df.cache()  
    count_total = df.count()
    count_distinct = df.distinct().count()       
    self.assertEqual(count_total, count_distinct)
  
  @parameterized.expand(
    get_test_fk_is_not_null_args(table_details), 
    skip_on_empty=True
  )
  def test_fk_is_not_null(self, name, table_name, keys, source_name):
    
    df = spark.table(table_name) \
      .filter(f"_SOURCE = '{source_name}'") \
      .select(keys)
    
    df.cache()  
    count_total = df.count()
    count_no_nulls = df.na.drop(how = 'any').count()
    self.assertEqual(count_total, count_no_nulls)
  
  @parameterized.expand(
    get_test_has_fk_record_args(table_details, 's_core.account_agg', 'account_ID'), 
    skip_on_empty=True
  )
  def test_has_account_record(self, name, table_name, source_name):
    result = spark.sql(f"""
      SELECT
        COUNT(*) AS RESULT
      FROM {table_name} f
      LEFT JOIN s_core.account_agg u ON f.account_ID = u._ID
      WHERE 
        u._ID IS NULL
        AND f._DELETED IS FALSE
        AND f._SOURCE = '{source_name}'
    """).collect()[0]['RESULT']
    self.assertEqual(result, 0)
  
  @parameterized.expand(
    get_test_has_fk_record_args(table_details, 's_core.groupings_agg', 'businessGroup_ID'), 
    skip_on_empty=True
  )
  def test_has_business_group_record(self, name, table_name, source_name):
    result = spark.sql(f"""
      SELECT
        COUNT(*) AS RESULT
      FROM {table_name} f
      LEFT JOIN s_core.groupings_agg u ON f.businessGroup_ID = u._ID
      WHERE 
        u._ID IS NULL
        AND f._DELETED IS FALSE
        AND f._SOURCE = '{source_name}'
    """).collect()[0]['RESULT']
    self.assertEqual(result, 0)  
  
  @parameterized.expand(
    get_test_has_fk_record_args(table_details, 's_core.user_agg', 'createdBy_ID'), 
    skip_on_empty=True
  )
  def test_has_created_by_record(self, name, table_name, source_name):
    result = spark.sql(f"""
      SELECT
        COUNT(*) AS RESULT
      FROM {table_name} f
      LEFT JOIN s_core.user_agg u ON f.createdBy_ID = u._ID
      WHERE 
        u._ID IS NULL
        AND f._DELETED IS FALSE
        AND f._SOURCE = '{source_name}'
    """).collect()[0]['RESULT']
    self.assertEqual(result, 0)
    
  @parameterized.expand(
    get_test_has_fk_record_args(table_details, 's_core.country_agg', 'countrycode_ID'), 
    skip_on_empty=True
  )
  def test_has_countrycode_record(self, name, table_name, source_name):
    result = spark.sql(f"""
      SELECT
        COUNT(*) AS RESULT
      FROM {table_name} f
      LEFT JOIN s_core.country_agg u ON f.countrycode_ID = u._ID
      WHERE 
        u._ID IS NULL
        AND f._DELETED IS FALSE
        AND f._SOURCE = '{source_name}'
    """).collect()[0]['RESULT']
    self.assertEqual(result, 0)
  
  @parameterized.expand(
    get_test_has_fk_record_args(table_details, 's_core.account_agg', 'customer_ID'), 
    skip_on_empty=True
  )
  def test_has_customer_record(self, name, table_name, source_name):
    result = spark.sql(f"""
      SELECT
        COUNT(*) AS RESULT
      FROM {table_name} f
      LEFT JOIN s_core.account_agg u ON f.customer_ID = u._ID
      WHERE 
        u._ID IS NULL
        AND f._DELETED IS FALSE
        AND f._SOURCE = '{source_name}'
    """).collect()[0]['RESULT']
    self.assertEqual(result, 0)
    
  @parameterized.expand(
    get_test_has_fk_record_args(table_details, 's_core.supplier_account_agg', 'defaultSupplier_ID'), 
    skip_on_empty=True
  )
  def test_has_default_supplier_record(self, name, table_name, source_name):
    result = spark.sql(f"""
      SELECT
        COUNT(*) AS RESULT
      FROM {table_name} f
      LEFT JOIN s_core.supplier_account_agg u ON f.defaultSupplier_ID = u._ID
      WHERE 
        u._ID IS NULL
        AND f._DELETED IS FALSE
        AND f._SOURCE = '{source_name}'
    """).collect()[0]['RESULT']
    self.assertEqual(result, 0)
  
  @parameterized.expand(
    get_test_has_fk_record_args(table_details, 's_core.product_agg', 'foreignCustomerItem_Id'), 
    skip_on_empty=True
  )
  def test_has_foreign_customer_item_record(self, name, table_name, source_name):
    result = spark.sql(f"""
      SELECT
        COUNT(*) AS RESULT
      FROM {table_name} f
      LEFT JOIN s_core.product_agg u ON f.foreignCustomerItem_Id = u._ID
      WHERE 
        u._ID IS NULL
        AND f._DELETED IS FALSE
        AND f._SOURCE = '{source_name}'
    """).collect()[0]['RESULT']
    self.assertEqual(result, 0)
  
  @parameterized.expand(
    get_test_has_fk_record_args(table_details, 's_core.product_agg', 'internalItem_Id'), 
    skip_on_empty=True
  )
  def test_has_internal_item_record(self, name, table_name, source_name):
    result = spark.sql(f"""
      SELECT
        COUNT(*) AS RESULT
      FROM {table_name} f
      LEFT JOIN s_core.product_agg u ON f.internalItem_Id = u._ID
      WHERE 
        u._ID IS NULL
        AND f._DELETED IS FALSE
        AND f._SOURCE = '{source_name}'
    """).collect()[0]['RESULT']
    self.assertEqual(result, 0)
  
  @parameterized.expand(
    get_test_has_fk_record_args(table_details, 's_core.product_agg', 'item_ID'), 
    skip_on_empty=True
  )
  def test_has_item_record(self, name, table_name, source_name):
    result = spark.sql(f"""
      SELECT
        COUNT(*) AS RESULT
      FROM {table_name} f
      LEFT JOIN s_core.product_agg u ON f.item_ID = u._ID
      WHERE 
        u._ID IS NULL
        AND f._DELETED IS FALSE
        AND f._SOURCE = '{source_name}'
    """).collect()[0]['RESULT']
    self.assertEqual(result, 0)
    
  @parameterized.expand(
    get_test_has_fk_record_args(table_details, 's_core.product_origin_agg', 'item_ID'), 
    skip_on_empty=True
  )
  def test_has_item_origin_record(self, name, table_name, source_name):
    result = spark.sql(f"""
      SELECT
        COUNT(*) AS RESULT
      FROM {table_name} f
      LEFT JOIN s_core.product_origin_agg u ON f.item_ID = u._ID
      WHERE 
        u._ID IS NULL
        AND f._DELETED IS FALSE
        AND f._SOURCE = '{source_name}'
    """).collect()[0]['RESULT']
    self.assertEqual(result, 0)
    
  @parameterized.expand(
    get_test_has_fk_record_args(table_details, 's_core.ledger_agg', 'ledger_ID'), 
    skip_on_empty=True
  )
  def test_has_ledger_record(self, name, table_name, source_name):
    result = spark.sql(f"""
      SELECT
        COUNT(*) AS RESULT
      FROM {table_name} f
      LEFT JOIN s_core.ledger_agg u ON f.ledger_ID = u._ID
      WHERE 
        u._ID IS NULL
        AND f._DELETED IS FALSE
        AND f._SOURCE = '{source_name}'
    """).collect()[0]['RESULT']
    self.assertEqual(result, 0)
  
  @parameterized.expand(
    get_test_has_fk_record_args(table_details, 's_core.user_agg', 'modifiedBy_ID'), 
    skip_on_empty=True
  )
  def test_has_modified_by_record(self, name, table_name, source_name):
    result = spark.sql(f"""
      SELECT
        COUNT(*) AS RESULT
      FROM {table_name} f
      LEFT JOIN s_core.user_agg u ON f.modifiedBy_ID = u._ID
      WHERE 
        u._ID IS NULL
        AND f._DELETED IS FALSE
        AND f._SOURCE = '{source_name}'
    """).collect()[0]['RESULT']
    self.assertEqual(result, 0)
  
  @parameterized.expand(
    get_test_has_fk_record_args(table_details, 's_core.organization_agg', 'organization_ID'), 
    skip_on_empty=True
  )
  def test_has_organization_record(self, name, table_name, source_name):
    result = spark.sql(f"""
      SELECT
        COUNT(*) AS RESULT
      FROM {table_name} f
      LEFT JOIN s_core.organization_agg u ON f.organization_ID = u._ID
      WHERE 
        u._ID IS NULL
        AND f._DELETED IS FALSE
        AND f._SOURCE = '{source_name}'
    """).collect()[0]['RESULT']
    self.assertEqual(result, 0)
      
  @parameterized.expand(
    get_test_has_fk_record_args(table_details, 's_core.product_origin_agg', 'organizationName_ID'), 
    skip_on_empty=True
  )
  def test_has_organization_name_record(self, name, table_name, source_name):
    result = spark.sql(f"""
      SELECT
        COUNT(*) AS RESULT
      FROM {table_name} f
      LEFT JOIN s_core.product_origin_agg u ON f.organizationName_ID = u._ID
      WHERE 
        u._ID IS NULL
        AND f._DELETED IS FALSE
        AND f._SOURCE = '{source_name}'
    """).collect()[0]['RESULT']
    self.assertEqual(result, 0)
    
  @parameterized.expand(
    get_test_has_fk_record_args(table_details, 's_core.payment_terms_agg', 'paymentTerm_ID'), 
    skip_on_empty=True
  )
  def test_has_payment_term_record(self, name, table_name, source_name):
    result = spark.sql(f"""
      SELECT
        COUNT(*) AS RESULT
      FROM {table_name} f
      LEFT JOIN s_core.payment_terms_agg u ON f.paymentTerm_ID = u._ID
      WHERE 
        u._ID IS NULL
        AND f._DELETED IS FALSE
        AND f._SOURCE = '{source_name}'
    """).collect()[0]['RESULT']
    self.assertEqual(result, 0)
    
  @parameterized.expand(
    get_test_has_fk_record_args(table_details, 's_core.payment_methods_agg', 'paymentMethod_ID'), 
    skip_on_empty=True
  )
  def test_has_payment_method_record(self, name, table_name, source_name):
    result = spark.sql(f"""
      SELECT
        COUNT(*) AS RESULT
      FROM {table_name} f
      LEFT JOIN s_core.payment_methods_agg u ON f.paymentMethod_ID = u._ID
      WHERE 
        u._ID IS NULL
        AND f._DELETED IS FALSE
        AND f._SOURCE = '{source_name}'
    """).collect()[0]['RESULT']
    self.assertEqual(result, 0)
    
  @parameterized.expand(
    get_test_has_fk_record_args(table_details, 's_core.product_origin_agg', 'origin_ID'), 
    skip_on_empty=True
  )
  def test_has_product_origin_record(self, name, table_name, source_name):
    result = spark.sql(f"""
      SELECT
        COUNT(*) AS RESULT
      FROM {table_name} f
      LEFT JOIN s_core.product_origin_agg u ON f.origin_ID = u._ID
      WHERE 
        u._ID IS NULL
        AND f._DELETED IS FALSE
        AND f._SOURCE = '{source_name}'
    """).collect()[0]['RESULT']
    self.assertEqual(result, 0)
  
  @parameterized.expand(
    get_test_has_fk_record_args(table_details, 's_core.origin_agg', 'origin_ID'), 
    skip_on_empty=True
  )
  def test_has_origin_record(self, name, table_name, source_name):
    result = spark.sql(f"""
      SELECT
        COUNT(*) AS RESULT
      FROM {table_name} f
      LEFT JOIN s_core.origin_agg u ON f.origin_ID = u._ID
      WHERE 
        u._ID IS NULL
        AND f._DELETED IS FALSE
        AND f._SOURCE = '{source_name}'
    """).collect()[0]['RESULT']
    self.assertEqual(result, 0)
  
  @parameterized.expand(
    get_test_has_fk_record_args(table_details, 's_core.organization_agg', 'owningBusinessUnit_ID'), 
    skip_on_empty=True
  )
  def test_has_owning_business_unit_record(self, name, table_name, source_name):
    result = spark.sql(f"""
      SELECT
        COUNT(*) AS RESULT
      FROM {table_name} f
      LEFT JOIN s_core.organization_agg o ON 
        f.owningBusinessUnit_ID = o._ID
        AND o.organizationType = 'OPERATING_UNIT'
      WHERE 
        o._ID IS NULL
        AND f._DELETED IS FALSE
        AND f._SOURCE = '{source_name}'
    """).collect()[0]['RESULT']
    self.assertEqual(result, 0)
  
  @parameterized.expand(
    get_test_has_fk_record_args(table_details, 's_core.party_agg', 'registration_ID'), 
    skip_on_empty=True
  )
  def test_has_registration_record(self, name, table_name, source_name):
    result = spark.sql(f"""
      SELECT
        COUNT(*) AS RESULT
      FROM {table_name} f
      LEFT JOIN s_core.party_agg u ON f.registration_ID = u._ID
      WHERE 
        u._ID IS NULL
        AND f._DELETED IS FALSE
        AND f._SOURCE = '{source_name}'
    """).collect()[0]['RESULT']
    self.assertEqual(result, 0)
    
  @parameterized.expand(
    get_test_has_fk_record_args(table_details, 's_core.supplier_location_agg', 'supplierSite_Id'), 
    skip_on_empty=True
  )
  def test_has_supplier_site_record(self, name, table_name, source_name):
    result = spark.sql(f"""
      SELECT
        COUNT(*) AS RESULT
      FROM {table_name} f
      LEFT JOIN s_core.supplier_location_agg u ON f.supplierSite_Id = u._ID
      WHERE 
        u._ID IS NULL
        AND f._DELETED IS FALSE
        AND f._SOURCE = '{source_name}'
    """).collect()[0]['RESULT']
    self.assertEqual(result, 0)
        
  @parameterized.expand(
    get_test_has_fk_record_args(table_details, 's_core.sales_organization_agg', 'salesorganization_ID'), 
    skip_on_empty=True
  )
  def test_has_salesorganizationid_record(self, name, table_name, source_name):
    result = spark.sql(f"""
      SELECT
        COUNT(*) AS RESULT
      FROM {table_name} f
      LEFT JOIN s_core.sales_organization_agg u ON f.salesorganization_ID = u._ID
      WHERE 
        u._ID IS NULL
        AND f._DELETED IS FALSE
        AND f._SOURCE = '{source_name}'
    """).collect()[0]['RESULT']
    self.assertEqual(result, 0)
    
  @parameterized.expand(
    get_test_has_fk_record_args(table_details, 's_core.territory_agg', 'territorycode_ID'), 
    skip_on_empty=True
  )
  def test_has_territorycode_record(self, name, table_name, source_name):
    result = spark.sql(f"""
      SELECT
        COUNT(*) AS RESULT
      FROM {table_name} f
      LEFT JOIN s_core.territory_agg u ON f.territorycode_ID = u._ID
      WHERE 
        u._ID IS NULL
        AND f._DELETED IS FALSE
        AND f._SOURCE = '{source_name}'
    """).collect()[0]['RESULT']
    self.assertEqual(result, 0)
   
  @parameterized.expand(
    get_test_has_fk_record_args(table_details, 's_core.supplier_account_agg', 'vendor_ID'), 
    skip_on_empty=True
  )
  def test_has_vendor_record(self, name, table_name, source_name):
    result = spark.sql(f"""
      SELECT
        COUNT(*) AS RESULT
      FROM {table_name} f
      LEFT JOIN s_core.supplier_account_agg u ON f.vendor_ID = u._ID
      WHERE 
        u._ID IS NULL
        AND f._DELETED IS FALSE
        AND f._SOURCE = '{source_name}'
    """).collect()[0]['RESULT']
    self.assertEqual(result, 0)
    
  def test_zip3_vertical_has_no_duplicates(self):
    df=spark.sql(f"""
      SELECT COUNT(*) 
      FROM s_core.territory_assignments_agg
      GROUP BY vertical,zip3,scenario
      HAVING COUNT(*) > 1""")
    self.assertEqual(df.count(), 0)
    
  @parameterized.expand(
    get_test_has_fk_record_args(table_details, 's_core.account_organization_agg', 'customerOrganization_ID'), 
    skip_on_empty=True
  )
  def test_has_customer_organization_record(self, name, table_name, source_name):
    result = spark.sql(f"""
      SELECT
        COUNT(*) AS RESULT
      FROM {table_name} f
      LEFT JOIN s_core.account_organization_agg u ON f.customerOrganization_ID = u._ID
      WHERE 
        u._ID IS NULL
        AND f._DELETED IS FALSE
        AND f._SOURCE = '{source_name}'
    """).collect()[0]['RESULT']
    self.assertEqual(result, 0)

# COMMAND ----------

suite = unittest.TestSuite()
suite.addTests(unittest.makeSuite(TestCore))
result = unittest.TextTestRunner(verbosity=3).run(suite)

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

# COMMAND ----------


