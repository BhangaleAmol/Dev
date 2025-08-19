# Databricks notebook source
def tg_default(source_name):
  def inner(df):
    return (
      df
      .transform(fix_dates)
      .transform(convert_empty_string_to_null)
      .transform(attach_source_column(source = source_name))
      .transform(attach_deleted_flag(False))
      .transform(attach_partition_column("createdOn"))
      .transform(attach_modified_date())
    )
  return inner

def tg_core_account():
  def inner(df):
    pk_columns = 'accountId,_SOURCE'
    return (
      df
      .transform(filter_null_values(columns = pk_columns))
      .transform(attach_primary_key(pk_columns, 'edm.account'))
      .transform(attach_key('createdBy,_SOURCE', 'createdBy_ID', 'edm.user'))
      .transform(attach_key('modifiedBy,_SOURCE', 'modifiedBy_ID', 'edm.user'))
      .transform(attach_surrogate_key(columns = 'registrationId,_SOURCE', name = "registration_ID"))
      .transform(attach_key('salesOrganizationID,_SOURCE', 'salesOrganization_ID', 'edm.salesorganization'))
      .transform(attach_surrogate_key(columns = 'territoryId,_SOURCE', name = "territory_ID"))
      .transform(attach_surrogate_key(columns = 'businessGroupId,_SOURCE', name = "businessGroup_ID"))
      .transform(attach_surrogate_key(columns = 'ownerId,_SOURCE', name = "owner_ID"))
    )
  return inner

def tg_core_average_unit_price():
  def inner(df):
    pk_columns = 'productStyle,subRegion,_SOURCE'
    return (
      df
      .transform(filter_null_values(columns = pk_columns))
      .transform(attach_surrogate_key(columns = pk_columns))
      .transform(attach_key('createdBy,_SOURCE', 'createdBy_ID', 'edm.user'))
      .transform(attach_key('modifiedBy,_SOURCE', 'modifiedBy_ID', 'edm.user'))
    )
  return inner

def tg_core_average_unit_price_fc():
  def inner(df):
    pk_columns = 'monthYear,productStyle,subRegion,_SOURCE'
    return (
      df
      .transform(filter_null_values(columns = pk_columns))
      .transform(attach_surrogate_key(columns = pk_columns))
      .transform(attach_key('createdBy,_SOURCE', 'createdBy_ID', 'edm.user'))
      .transform(attach_key('modifiedBy,_SOURCE', 'modifiedBy_ID', 'edm.user'))
    )
  return inner

def tg_core_bill_to():
  def inner(df):
    pk_columns = 'billToAddressId,_SOURCE'
    return (
      df
      .transform(filter_null_values(columns = pk_columns))
      .transform(attach_surrogate_key(columns = pk_columns))
      .transform(attach_key('createdBy,_SOURCE', 'createdBy_ID', 'edm.user'))
      .transform(attach_key('modifiedBy,_SOURCE', 'modifiedBy_ID', 'edm.user'))
      .transform(attach_key('accountId,_SOURCE', 'account_ID', 'edm.account'))
    )
  return inner

def tg_core_capacity_master(df):
  pk_columns = 'Line, CapacityGroup, Size, Location, ResourceType, Style, UOM, BeginDate, _SOURCE'
  return (
    df
    .transform(attach_primary_key(pk_columns, 'edm.capacity_master'))
  )

def tg_core_customer_items():
  def inner(df):
    pk_columns = 'customerId,foreignCustomerItemNumber,preferenceNumber,_SOURCE'
    return (
      df
      .transform(filter_null_values(columns = pk_columns))
      .transform(attach_surrogate_key(columns = pk_columns))
      .transform(attach_key('customerId,_SOURCE', 'customer_ID', 'edm.account'))
      .transform(attach_surrogate_key(columns = 'foreignCustomerItemId,_SOURCE', name = "foreignCustomerItem_Id"))
      .transform(attach_surrogate_key(columns = 'internalItemId,_SOURCE', name = "internalItem_Id"))
      .transform(attach_key('createdBy,_SOURCE', 'createdBy_ID', 'edm.user'))
      .transform(attach_key('modifiedBy,_SOURCE', 'modifiedBy_ID', 'edm.user'))
    )
  return inner

def tg_core_customer_location():
  def inner(df):
    pk_columns = 'addressId,_SOURCE'
    return (
      df
      .transform(filter_null_values(columns = pk_columns))
      .transform(attach_primary_key(pk_columns, 'edm.customer_location'))
      .transform(attach_surrogate_key(columns = 'country,_SOURCE', name = "territory_ID"))
      .transform(attach_key('countryCode,_SOURCE', 'countryCode_ID', 'edm.country'))
      .transform(attach_key('createdBy,_SOURCE', 'createdBy_ID', 'edm.user'))
      .transform(attach_key('modifiedBy,_SOURCE', 'modifiedBy_ID', 'edm.user'))
      .transform(attach_key('accountId,_SOURCE', 'account_ID', 'edm.account'))
    )
  return inner

def tg_core_country():
  def inner(df):
    pk_columns = 'countryCode,_SOURCE'
    return (
      df
      .transform(filter_null_values(columns = pk_columns))
      .transform(attach_primary_key('countryCode,_SOURCE', 'edm.country'))
      .transform(attach_key('createdBy,_SOURCE', 'createdBy_ID', 'edm.user'))
      .transform(attach_key('modifiedBy,_SOURCE', 'modifiedBy_ID', 'edm.user'))
      .transform(sort_columns)
    )
  return inner

def tg_core_exchange_rate():
  def inner(df):
    pk_columns = 'fxID,_SOURCE'
    return (
      df
      .transform(filter_null_values(columns = pk_columns))
      .transform(attach_surrogate_key(columns = pk_columns, name='_ID'))
      .transform(attach_key('createdBy,_SOURCE', 'createdBy_ID', 'edm.user'))
      .transform(attach_key('modifiedBy,_SOURCE', 'modifiedBy_ID', 'edm.user'))
    )
  return inner

def tg_core_groupings():
  def inner(df):
    pk_columns = 'groupId,_SOURCE'
    return (
      df
      .transform(filter_null_values(columns = pk_columns))
      .transform(attach_surrogate_key(columns = pk_columns))
      .transform(attach_key('createdBy,_SOURCE', 'createdBy_ID', 'edm.user'))
      .transform(attach_key('modifiedBy,_SOURCE', 'modifiedBy_ID', 'edm.user'))
    )
  return inner

def tg_core_organization():
  def inner(df):
    pk_columns = 'organizationId,_SOURCE'
    return (
      df
      .transform(filter_null_values(columns = pk_columns))
      .transform(attach_surrogate_key(columns = pk_columns))
      .transform(attach_surrogate_key(columns = 'country,_SOURCE', name = "territory_ID"))
      .transform(attach_key('countryCode,_SOURCE', 'countryCode_ID', 'edm.country'))
      .transform(attach_key('createdBy,_SOURCE', 'createdBy_ID', 'edm.user'))
      .transform(attach_key('modifiedBy,_SOURCE', 'modifiedBy_ID', 'edm.user'))
      .transform(sort_columns)
    )
  return inner

def tg_core_party():
  def inner(df):
    pk_columns = 'partyId,_SOURCE'
    return (
      df
      .transform(filter_null_values(columns = pk_columns))
      .transform(attach_surrogate_key(columns = pk_columns))
      .transform(attach_key('createdBy,_SOURCE', 'createdBy_ID', 'edm.user'))
      .transform(attach_key('modifiedBy,_SOURCE', 'modifiedBy_ID', 'edm.user'))
      .transform(attach_surrogate_key(columns = 'vendorId,_SOURCE', name = "vendor_ID"))
      .transform(sort_columns)
    )
  return inner

def tg_core_product():
  def inner(df):
    pk_columns = 'itemId,_SOURCE'
    return (
      df    
      .transform(filter_null_values(columns = pk_columns))
      .transform(convert_null_to_unknown(columns = 'brandFamily,brandStrategy,gbu,legacyAspn,lowestShippableUom,marketingCode,primaryTechnology,productBrand,productCategory,productDivision,productFamily,productM4Category,productM4Family,productM4Group,productM4Segment,productNps,productSbu,productStatus,productStyle,productSubBrand,productVariant'))
      .transform(attach_surrogate_key(columns = pk_columns))
      .transform(attach_key('createdBy,_SOURCE', 'createdBy_ID', 'edm.user'))
      .transform(attach_key('modifiedBy,_SOURCE', 'modifiedBy_ID', 'edm.user'))
      .transform(attach_key('originId,_SOURCE', 'origin_ID', 'edm.origin'))
      .transform(sort_columns)
    )
  return inner

def tg_core_product_region():
  def inner(df):
    pk_columns = 'itemId,region,_SOURCE'
    return (
      df    
      .transform(filter_null_values(columns = pk_columns))
      .transform(convert_null_to_unknown(columns = 'ansStdUom,gbu,name,productBrand,productCode,productSBU,productStyle,sizeDescription'))
      .transform(attach_surrogate_key(columns = pk_columns))
      .transform(attach_key('createdBy,_SOURCE', 'createdBy_ID', 'edm.user'))
      .transform(attach_key('modifiedBy,_SOURCE', 'modifiedBy_ID', 'edm.user'))
      .transform(sort_columns)
    )
  return inner

def tg_core_sales_budget():
  def inner(df):
    pk_columns = 'budgetCustomerCode, budgetMonth,  owningBusinessUnitId, budgetFiscalYear,_SOURCE'
    return (
      df
      .transform(filter_null_values(columns = pk_columns))
      .transform(attach_surrogate_key(columns = pk_columns))      
      .transform(attach_key('createdBy,_SOURCE', 'createdBy_ID', 'edm.user'))
      .transform(attach_key('modifiedBy,_SOURCE', 'modifiedBy_ID', 'edm.user'))
      .transform(attach_key('budgetCustomerCode,_SOURCE', 'account_ID', 'edm.account'))
      .transform(attach_surrogate_key(columns = 'budgetProductCode,_SOURCE', name = "item_ID"))
      .transform(attach_surrogate_key(columns = 'owningBusinessUnitId,_SOURCE', name='owningBusinessUnit_ID'))
      .transform(sort_columns)
    )
  return inner

def tg_core_sales_organization():
  def inner(df):
    pk_columns = 'salesOrganizationId,_SOURCE'
    return (
      df
      .transform(filter_null_values(columns = pk_columns))
      .transform(attach_primary_key(pk_columns, 'edm.salesorganization'))
      .transform(attach_key('createdBy,_SOURCE', 'createdBy_ID', 'edm.user'))
      .transform(attach_key('modifiedBy,_SOURCE', 'modifiedBy_ID', 'edm.user'))
      .transform(sort_columns)
    )
  return inner


def tg_core_supplier_account():
  def inner(df):
    pk_columns = 'supplierId,_SOURCE'
    return (
      df
      .transform(filter_null_values(columns = pk_columns))
      .transform(attach_surrogate_key(columns = pk_columns, name='_ID'))
      .transform(attach_key('createdBy,_SOURCE', 'createdBy_ID', 'edm.user'))
      .transform(attach_key('modifiedBy,_SOURCE', 'modifiedBy_ID', 'edm.user'))
      .transform(sort_columns)
    )
  return inner

def tg_core_supplier_location():
  def inner(df):
    pk_columns = 'supplierSiteId,_SOURCE'
    return (
      df
      .transform(filter_null_values(columns = pk_columns))
      .transform(attach_surrogate_key(columns = pk_columns, name='_ID'))
      .transform(attach_key('createdBy,_SOURCE', 'createdBy_ID', 'edm.user'))
      .transform(attach_key('modifiedBy,_SOURCE', 'modifiedBy_ID', 'edm.user'))
      .transform(sort_columns)
    )
  return inner


def tg_core_territory_assignments():
  def inner(df):
    pk_columns = 'vertical,zip3,scenario,_SOURCE'
    return (
      df
      .transform(filter_null_values(columns = pk_columns))
      .transform(attach_surrogate_key(columns = pk_columns))  
      .transform(attach_key('createdBy,_SOURCE', 'createdBy_ID', 'edm.user'))
      .transform(attach_key('modifiedBy,_SOURCE', 'modifiedBy_ID', 'edm.user'))
      .transform(sort_columns)
    )
  return inner

def tg_core_time():
  def inner(df):
    pk_columns = 'dateID'
    return (
      df
      .transform(filter_null_values(columns = pk_columns))
      .transform(attach_surrogate_key(columns = pk_columns))  
      .transform(attach_key('createdBy,_SOURCE', 'createdBy_ID', 'edm.user'))
      .transform(attach_key('modifiedBy,_SOURCE', 'modifiedBy_ID', 'edm.user'))
      .transform(sort_columns)
    )
  return inner

def tg_core_transaction_type():
  def inner(df):
    pk_columns = 'transactionId,transactionGroup,_SOURCE'
    return (
      df
      .transform(filter_null_values(columns = pk_columns))
      .transform(attach_surrogate_key(columns = pk_columns))
      .transform(attach_key('createdBy,_SOURCE', 'createdBy_ID', 'edm.user'))
      .transform(attach_key('modifiedBy,_SOURCE', 'modifiedBy_ID', 'edm.user'))
      .transform(sort_columns)
    )
  return inner

def tg_core_user():
  def inner(df):
    pk_columns = 'personId,_SOURCE'
    return (
      df      
      .transform(filter_null_values(columns = pk_columns))
      .transform(attach_primary_key(pk_columns, 'edm.user'))
      .transform(attach_key('createdBy,_SOURCE', 'createdBy_ID', 'edm.user'))
      .transform(attach_key('modifiedBy,_SOURCE', 'modifiedBy_ID', 'edm.user'))
      .transform(attach_key('accountId,_SOURCE', 'account_ID', 'edm.account'))
      .transform(sort_columns)
    )
  return inner

def tg_core_origin():
  def inner(df):
    pk_columns = 'originId,_SOURCE'
    return (
      df
      .transform(filter_null_values(columns = pk_columns))
      .transform(attach_primary_key(pk_columns, 'edm.origin'))
      .transform(attach_key('createdBy,_SOURCE', 'createdBy_ID', 'edm.user'))
      .transform(attach_key('modifiedBy,_SOURCE', 'modifiedBy_ID', 'edm.user'))
      .transform(sort_columns)
    )
  return inner

def tg_core_product_origin():
  def inner(df):
    pk_columns = 'itemId,originId,organizationName,_SOURCE'
    return (
      df
      .transform(filter_null_values(columns = pk_columns))
      .transform(attach_surrogate_key(columns = pk_columns))
      .transform(attach_key('originId,_SOURCE', 'origin_ID', 'edm.origin'))
      .transform(attach_surrogate_key(columns = 'itemId,_SOURCE', name = "item_ID"))
      .transform(attach_key('createdBy,_SOURCE', 'createdBy_ID', 'edm.user'))
      .transform(attach_key('modifiedBy,_SOURCE', 'modifiedBy_ID', 'edm.user'))
      .transform(sort_columns)
    )
  return inner

def tg_core_product_org():
  def inner(df):
    pk_columns = 'itemId,organizationId,_SOURCE'
    return (
      df
      .transform(filter_null_values(columns = pk_columns))
      .transform(attach_surrogate_key(columns = pk_columns))
      .transform(attach_surrogate_key(columns = 'itemId,_SOURCE', name = "item_ID"))
      .transform(attach_surrogate_key(columns = 'organizationId,_SOURCE', name = "organization_ID"))
      .transform(attach_surrogate_key(columns = 'defaultSupplierId,_SOURCE', name = "defaultSupplier_ID"))
      .transform(attach_key('createdBy,_SOURCE', 'createdBy_ID', 'edm.user'))
      .transform(attach_key('modifiedBy,_SOURCE', 'modifiedBy_ID', 'edm.user'))
      .transform(sort_columns)
    )
  return inner

def tg_core_ledger():
  def inner(df):
    pk_columns = 'ledgerId,_SOURCE'
    return (
      df
      .transform(filter_null_values(columns = pk_columns))
      .transform(attach_surrogate_key(columns = pk_columns))
      .transform(attach_key('createdBy,_SOURCE', 'createdBy_ID', 'edm.user'))
      .transform(attach_key('modifiedBy,_SOURCE', 'modifiedBy_ID', 'edm.user'))
    )
  return inner

def tg_core_payment_terms():
  def inner(df):
    pk_columns = 'paymentTermId,_SOURCE'
    return (
      df
      .transform(filter_null_values(columns = pk_columns))
      .transform(attach_surrogate_key(columns = pk_columns))
      .transform(attach_key('createdBy,_SOURCE', 'createdBy_ID', 'edm.user'))
      .transform(attach_key('modifiedBy,_SOURCE', 'modifiedBy_ID', 'edm.user'))
      .transform(sort_columns)
    )
  return inner

def tg_core_payment_methods():
  def inner(df):
    pk_columns = 'paymentMethodId,_SOURCE'
    return (
      df
      .transform(filter_null_values(columns = pk_columns))
      .transform(attach_surrogate_key(columns = pk_columns))
      .transform(attach_surrogate_key(columns = 'createdBy,_SOURCE', name = "createdBy_ID"))
      .transform(attach_surrogate_key(columns = 'modifiedBy,_SOURCE', name = "modifiedBy_ID"))
      .transform(sort_columns)
    )
  return inner

def tg_core_territory():
  def inner(df):
    pk_columns = 'territorycode,_SOURCE'
    return (
      df
      .transform(filter_null_values(columns = pk_columns))
      .transform(attach_surrogate_key(columns = pk_columns))
      .transform(attach_key('createdBy,_SOURCE', 'createdBy_ID', 'edm.user'))
      .transform(attach_key('modifiedBy,_SOURCE', 'modifiedBy_ID', 'edm.user'))
      .transform(sort_columns)
    )
  return inner

def tg_core_customer_sales_organization():
  def inner(df):
    pk_columns = 'accountId, regionManager, territoryManager,_SOURCE'
    return (
      df
      .transform(filter_null_values(columns = pk_columns))
      .transform(attach_surrogate_key(columns = pk_columns))
      .transform(attach_surrogate_key(columns = 'businessGroupId,_SOURCE', name  = 'businessGroup_ID'))
      .transform(attach_key('createdBy,_SOURCE', 'createdBy_ID', 'edm.user'))
      .transform(attach_key('modifiedBy,_SOURCE', 'modifiedBy_ID', 'edm.user'))
      .transform(attach_key('accountId,_SOURCE', 'account_ID', 'edm.account'))
    )
  return inner

def tg_core_sales_forecast():
  def inner(df):
    pk_columns ='budgetCustomerCode, budgetProductCode, budgetFiscalYear, budgetMonth,  owningBusinessUnitId,_SOURCE'
    return (
      df
      .transform(filter_null_values(columns = pk_columns))
      .transform(attach_surrogate_key(columns = pk_columns))
      .transform(attach_surrogate_key(columns = 'owningBusinessUnitId,_SOURCE', name = "owningBusinessUnit_Id"))
      .transform(attach_surrogate_key(columns = 'budgetProductCode,_SOURCE', name = "item_ID"))
      .transform(attach_surrogate_key(columns = 'budgetCustomerCode,_SOURCE', name = "account_ID"))
      .transform(attach_key('createdBy,_SOURCE', 'createdBy_ID', 'edm.user'))
      .transform(attach_key('modifiedBy,_SOURCE', 'modifiedBy_ID', 'edm.user'))
      .transform(attach_key('budgetCustomerCode,_SOURCE', 'account_ID', 'edm.account'))
      .transform(sort_columns)
    )
  return inner

def tg_core_account_organization():
  def inner(df):
    pk_columns ='accountId, customerDivision, salesOrganization, distributionChannel,_SOURCE'
    return (
      df
      .transform(filter_null_values(columns = pk_columns))
      .transform(attach_primary_key(pk_columns,'edm.account_organization'))
      .transform(attach_key('createdBy,_SOURCE', 'createdBy_ID', 'edm.user'))
      .transform(attach_key('modifiedBy,_SOURCE', 'modifiedBy_ID', 'edm.user'))
      .transform(attach_key('accountId,_SOURCE', 'account_ID', 'edm.account'))
      .transform(sort_columns)
    )
  return inner

def tg_core_substitution_master():
  def inner(df):
    pk_columns ='substitutionId,_SOURCE'
    return (
      df
      .transform(filter_null_values(columns = pk_columns))
      .transform(attach_surrogate_key(columns = pk_columns))
      .transform(attach_key('createdBy,_SOURCE', 'createdBy_ID', 'edm.user'))
      .transform(attach_key('modifiedBy,_SOURCE', 'modifiedBy_ID', 'edm.user'))
      .transform(sort_columns)
    )
  return inner

def tg_finance_ap_transactions():
  def inner(df):
    pk_columns = 'transactionDetailId,_SOURCE'
    return (
      df
      .transform(filter_null_values(columns = pk_columns))
      .transform(attach_surrogate_key(columns = pk_columns))
      .transform(attach_key('createdBy,_SOURCE', 'createdBy_ID', 'edm.user'))
      .transform(attach_key('modifiedBy,_SOURCE', 'modifiedBy_ID', 'edm.user'))
      .transform(attach_surrogate_key(columns = 'itemId,_SOURCE', name = "item_ID"))
      .transform(attach_surrogate_key(columns = 'supplierSiteId,_SOURCE', name = "supplierSite_ID"))
      .transform(attach_surrogate_key(columns = 'supplierId,_SOURCE', name = "supplier_ID"))
      .transform(attach_surrogate_key(columns = 'companyId,_SOURCE', name = "company_ID"))
      .transform(attach_surrogate_key(columns = 'costCenterId,_SOURCE', name = "costCenter_ID"))
      .transform(attach_surrogate_key(columns = 'glAccountId,_SOURCE', name = "glAccount_ID"))
      .transform(attach_surrogate_key(columns = 'glBalanceId,_SOURCE', name = "glBalance_ID"))
      .transform(attach_surrogate_key(columns = 'invoiceId,_SOURCE', name = "invoice_ID"))
      .transform(attach_surrogate_key(columns = 'ledgerId,_SOURCE', name = "ledger_ID"))
      .transform(attach_surrogate_key(columns = 'owningBusinessUnitId,_SOURCE', name = "owningBusinessUnit_ID"))
      .transform(attach_surrogate_key(columns = 'paymentMethodId,_SOURCE', name = "paymentMethod_ID"))
      .transform(attach_surrogate_key(columns = 'paymentTermId,_SOURCE', name = "paymentTerm_ID"))
      .transform(attach_surrogate_key(columns = 'profitCenterId,_SOURCE', name = "profitCenter_ID"))
    )
  return inner

def tg_finance_ar_transactions():
  def inner(df):
    pk_columns = 'transactionDetailId,_SOURCE'
    return (
      df
      .transform(filter_null_values(columns = pk_columns))
      .transform(attach_surrogate_key(columns = pk_columns))
      .transform(attach_key('createdBy,_SOURCE', 'createdBy_ID', 'edm.user'))
      .transform(attach_key('modifiedBy,_SOURCE', 'modifiedBy_ID', 'edm.user'))
      .transform(attach_surrogate_key(columns = 'companyId,_SOURCE', name = "company_ID"))
      .transform(attach_surrogate_key(columns = 'costCenterId,_SOURCE', name = "costCenter_ID"))
      .transform(attach_surrogate_key(columns = 'glAccountId,_SOURCE', name = "glAccount_ID"))
      .transform(attach_surrogate_key(columns = 'invoiceId,_SOURCE', name = "invoice_ID"))
      .transform(attach_surrogate_key(columns = 'owningBusinessUnitId,_SOURCE', name = "owningBusinessUnit_ID"))
      .transform(attach_surrogate_key(columns = 'profitCenterId,_SOURCE', name = "profitCenter_ID"))
    )
  return inner

def tg_finance_gl_transactions():
  def inner(df):
    pk_columns = 'glJournalId,_SOURCE'
    return (
      df
      .transform(filter_null_values(columns = pk_columns))
      .transform(attach_surrogate_key(columns = pk_columns))
      .transform(attach_key('createdBy,_SOURCE', 'createdBy_ID', 'edm.user'))
      .transform(attach_key('modifiedBy,_SOURCE', 'modifiedBy_ID', 'edm.user'))
      .transform(attach_surrogate_key(columns = 'companyId,_SOURCE', name = "company_ID"))
      .transform(attach_surrogate_key(columns = 'costCenterId,_SOURCE', name = "costCenter_ID"))
      .transform(attach_surrogate_key(columns = 'glAccountId,_SOURCE', name = "glAccount_ID"))
      .transform(attach_surrogate_key(columns = 'ledgerId,_SOURCE', name = "ledger_ID"))
      .transform(attach_surrogate_key(columns = 'legalEntityId,_SOURCE', name = "legalEntity_ID"))
      .transform(attach_surrogate_key(columns = 'profitCenterId,_SOURCE', name = "profitCenter_ID"))
      .transform(attach_surrogate_key(columns = 'divisionId,_SOURCE', name = "division_ID"))
      .transform(attach_surrogate_key(columns = 'regionId,_SOURCE', name = "region_ID"))
      .transform(attach_surrogate_key(columns = 'interCompanyId,_SOURCE', name = "interCompany_ID"))
      .transform(attach_surrogate_key(columns = 'futureId,_SOURCE', name = "future_ID"))
    )
  return inner



def tg_finance_gl_accounts():
  def inner(df):
    pk_columns = 'glCombinationId, _SOURCE'
    return (
      df
      .transform(filter_null_values(columns = pk_columns))
      .transform(attach_primary_key(pk_columns, 'edm.gl_accounts'))
      .transform(attach_key('createdBy,_SOURCE', 'createdBy_ID', 'edm.user'))
      .transform(attach_key('modifiedBy,_SOURCE', 'modifiedBy_ID', 'edm.user'))
    )
  return inner

def tg_finance_gl_segments():
  def inner(df):
    pk_columns = 'segmentId, _SOURCE'
    return (
      df
      .transform(filter_null_values(columns = pk_columns))
      .transform(attach_surrogate_key(columns = pk_columns, name='_ID'))
      .transform(attach_key('createdBy,_SOURCE', 'createdBy_ID', 'edm.user'))
      .transform(attach_key('modifiedBy,_SOURCE', 'modifiedBy_ID', 'edm.user'))
    )
  return inner




def tg_service_business_overview():
  def inner(df):
    pk_columns = 'businessoverviewId,_SOURCE'
    return (
      df
      .transform(attach_primary_key(pk_columns, 'edm.business_overview'))
      .transform(attach_key('createdBy,_SOURCE', 'createdBy_ID', 'edm.user'))
      .transform(attach_key('modifiedBy,_SOURCE', 'modifiedBy_ID', 'edm.user'))
      .transform(attach_surrogate_key(columns = 'linkedOpportunity,_SOURCE', name='linkedOpportunity_ID'))
      .transform(attach_surrogate_key(columns = 'recordTypeId,orderType,_SOURCE', name='recordType_ID'))
      .transform(sort_columns)
    )
  return inner




def tg_service_business_product():
  def inner(df):
    pk_columns = 'businessproductId,_SOURCE'
    return (
      df
      .transform(attach_primary_key(pk_columns, 'edm.business_product'))
      .transform(attach_key('createdBy,_SOURCE', 'createdBy_ID', 'edm.user'))
      .transform(attach_key('modifiedBy,_SOURCE', 'modifiedBy_ID', 'edm.user'))
      .transform(attach_key('businessOverview,_SOURCE','businessOverview_ID','edm.business_overview'))
      .transform(attach_surrogate_key(columns = 'product,_SOURCE', name='product_ID'))
      .transform(sort_columns)
    )
  return inner



def tg_service_case():
  def inner(df):
    return (
      df
      .transform(attach_surrogate_key(columns = 'caseId,_SOURCE'))
      .transform(attach_key('createdBy,_SOURCE', 'createdBy_ID', 'edm.user'))
      .transform(attach_key('modifiedBy,_SOURCE', 'modifiedBy_ID', 'edm.user'))
      .transform(attach_key('accountID,_SOURCE','account_ID','edm.account'))
      .transform(attach_key('ownerId,_SOURCE','owner_ID', 'edm.user'))
      .transform(attach_surrogate_key(columns = 'recordTypeId,orderType,_SOURCE', name='recordType_ID'))
      .transform(sort_columns)
    )
  return inner


def tg_service_lost_business():
  def inner(df):
    pk_columns = 'lostbusinessId,_SOURCE'
    return (
      df
      .transform(attach_primary_key(pk_columns, 'edm.lost_business'))
      .transform(attach_key('createdBy,_SOURCE', 'createdBy_ID', 'edm.user'))
      .transform(attach_key('modifiedBy,_SOURCE', 'modifiedBy_ID', 'edm.user'))
      .transform(attach_key('ownerId,_SOURCE','owner_ID', 'edm.user'))
      .transform(attach_surrogate_key(columns = 'recordTypeId,orderType,_SOURCE', name='recordType_ID'))
      .transform(sort_columns)
    )
  return inner



def tg_service_lost_business_lines():
  def inner(df):
    pk_columns = 'lostbusinessLineId,_SOURCE'
    return (
      df
      .transform(attach_primary_key(pk_columns, 'edm.lost_business_lines'))
      .transform(attach_key('createdBy,_SOURCE', 'createdBy_ID', 'edm.user'))
      .transform(attach_key('modifiedBy,_SOURCE', 'modifiedBy_ID', 'edm.user'))
      .transform(attach_surrogate_key(columns = 'itemId,_SOURCE', name='item_ID'))
      .transform(attach_key('lostbusinessId,_SOURCE','lostbusiness_ID', 'edm.lost_business'))
      .transform(sort_columns)
    )
  return inner


def tg_service_opportunity():
  def inner(df):
    return (
      df
      .transform(attach_surrogate_key(columns = 'opportunityId,_SOURCE'))
      .transform(attach_key('createdBy,_SOURCE', 'createdBy_ID', 'edm.user'))
      .transform(attach_key('modifiedBy,_SOURCE', 'modifiedBy_ID', 'edm.user'))
      .transform(attach_key('accountID,_SOURCE','account_ID','edm.account'))
      .transform(attach_key('distributor,_SOURCE','distributor_ID','edm.account'))
      .transform(attach_key('ownerId,_SOURCE','owner_ID', 'edm.user'))
      .transform(sort_columns)
    )
  return inner


def tg_service_opportunity_lines():
  def inner(df):
    return (
      df
      .transform(attach_surrogate_key(columns = 'optyProductId,_SOURCE'))
      .transform(attach_key('createdBy,_SOURCE', 'createdBy_ID', 'edm.user'))
      .transform(attach_key('modifiedBy,_SOURCE', 'modifiedBy_ID', 'edm.user'))
      .transform(attach_surrogate_key(columns = 'productId,_SOURCE', name='product_ID'))
      .transform(attach_surrogate_key(columns = 'opportunityId,_SOURCE', name='opportunity_ID'))
      .transform(sort_columns)
    )
  return inner


def tg_service_quality_complaint_defects():
  def inner(df):
    pk_columns = 'complaintTypeId,_SOURCE'
    return (
      df
      .transform(filter_null_values(columns = pk_columns))
      .transform(attach_surrogate_key(columns = pk_columns, name='_ID'))
      .transform(attach_key('createdBy,_SOURCE', 'createdBy_ID', 'edm.user'))
      .transform(attach_key('modifiedBy,_SOURCE', 'modifiedBy_ID', 'edm.user'))
      .transform(attach_surrogate_key(columns = 'complaintid,_SOURCE', name='complaint_ID'))
    )
  return inner

def tg_service_quality_complaint():
  def inner(df):
    pk_columns = 'complaintId,_SOURCE'
    return (
      df
      .transform(filter_null_values(columns = pk_columns))
      .transform(attach_surrogate_key(columns = pk_columns))
      .transform(attach_key('createdBy,_SOURCE', 'createdBy_ID', 'edm.user'))
      .transform(attach_key('modifiedBy,_SOURCE', 'modifiedBy_ID', 'edm.user'))   
      .transform(attach_surrogate_key(columns = 'assignedToId,_SOURCE', name='assignedTo_ID'))
      .transform(attach_surrogate_key(columns = 'caseId,_SOURCE', name='case_ID'))
      .transform(attach_surrogate_key(columns = 'complaintApproverId,_SOURCE', name='complaintApprover_ID'))
      .transform(attach_surrogate_key(columns = 'complaintCAPAId,_SOURCE', name='complaintCapa_ID'))
      .transform(attach_surrogate_key(columns = 'complaintInvestigatorId,_SOURCE', name='complaintInvestigator_ID'))
      .transform(attach_surrogate_key(columns = 'complaintProductId,_SOURCE', name='complaintProduct_ID'))
      .transform(attach_surrogate_key(columns = 'optionalApproverId,_SOURCE', name='optionalApprover_ID'))
      .transform(attach_surrogate_key(columns = 'ownerID,_SOURCE', name='owner_ID'))
    )
  return inner

def tg_service_quality_creditnote_approval():
  def inner(df):
    pk_columns = 'complaintId,_SOURCE'
    return (
      df
      .transform(filter_null_values(columns = pk_columns))
      .transform(attach_surrogate_key(columns = pk_columns, name='_ID'))
      .transform(attach_key('createdBy,_SOURCE', 'createdBy_ID', 'edm.user'))
      .transform(attach_key('modifiedBy,_SOURCE', 'modifiedBy_ID', 'edm.user'))
    )
  return inner

def tg_service_service_complaint():
  def inner(df):
    pk_columns = 'complaintId,_SOURCE'
    return (
      df
      .transform(filter_null_values(columns = pk_columns))
      .transform(attach_surrogate_key(columns = pk_columns))
      .transform(attach_key('createdBy,_SOURCE', 'createdBy_ID', 'edm.user'))
      .transform(attach_key('modifiedBy,_SOURCE', 'modifiedBy_ID', 'edm.user'))
    )
  return inner

def tg_supplychain_buyer():
  def inner(df):
    pk_columns = 'buyerId,_SOURCE'
    return (
      df
      .transform(filter_null_values(columns = pk_columns))
      .transform(attach_surrogate_key(columns = pk_columns))
      .transform(attach_key('createdBy,_SOURCE', 'createdBy_ID', 'edm.user'))
      .transform(attach_key('modifiedBy,_SOURCE', 'modifiedBy_ID', 'edm.user'))
    )
  return inner

def tg_supplychain_inventory():
  def inner(df):
    pk_columns = 'itemID,inventoryWarehouseID,subInventoryCode,lotNumber,inventoryDate,owningBusinessUnitId,_SOURCE'
    return (
      df
      .transform(filter_null_values(columns = pk_columns))
      .transform(attach_surrogate_key(columns = pk_columns))
      .transform(attach_key('createdBy,_SOURCE', 'createdBy_ID', 'edm.user'))
      .transform(attach_key('modifiedBy,_SOURCE', 'modifiedBy_ID', 'edm.user'))
      .transform(attach_surrogate_key(columns = 'itemId,_SOURCE', name = "item_ID")) 
      .transform(attach_surrogate_key(columns = 'inventoryWarehouseID,_SOURCE', name = "inventoryWarehouse_ID")) 
      .transform(attach_key('originId,_SOURCE', 'origin_ID', 'edm.origin'))
      .transform(attach_surrogate_key(columns = 'owningBusinessUnitId,_SOURCE', name='owningBusinessUnit_ID'))
    )
  return inner

def tg_supplychain_purchaseorder():
  def inner(df):
    pk_columns = 'purchaseOrderId,_SOURCE'
    return (
      df
      .transform(filter_null_values(columns = pk_columns))
      .transform(attach_surrogate_key(columns = pk_columns))
      .transform(attach_surrogate_key(columns = 'buyerId,_SOURCE', name = "buyer_ID"))
      .transform(attach_key('createdBy,_SOURCE', 'createdBy_ID', 'edm.user'))
      .transform(attach_key('modifiedBy,_SOURCE', 'modifiedBy_ID', 'edm.user'))
      .transform(attach_surrogate_key(columns = 'supplierId,_SOURCE', name = "supplier_ID")) 
      .transform(attach_surrogate_key(columns = 'supplierSiteId,_SOURCE', name = "supplierSite_ID"))
      .transform(attach_surrogate_key(columns = 'owningBusinessUnitId,_SOURCE', name = "owningBusinessUnit_ID"))
    )
  return inner

def tg_supplychain_purchaseorderlines():
  def inner(df):
    pk_columns = 'purchaseOrderDetailId,_SOURCE'
    return (
      df
      .transform(filter_null_values(columns = pk_columns))
      .transform(attach_surrogate_key(columns = pk_columns))
      .transform(attach_key('createdBy,_SOURCE', 'createdBy_ID', 'edm.user'))
      .transform(attach_key('modifiedBy,_SOURCE', 'modifiedBy_ID', 'edm.user'))
      .transform(attach_surrogate_key(columns = 'inventoryWarehouseId,_SOURCE', name = "inventoryWarehouse_ID"))
      .transform(attach_surrogate_key(columns = 'itemId,_SOURCE', name = "item_ID")) 
      .transform(attach_surrogate_key(columns = 'purchaseOrderId,_SOURCE', name = "purchaseOrder_ID"))
    )
  return inner

def tg_supplychain_sales_invoice_headers():
  def inner(df):
    pk_columns = 'invoiceId,owningBusinessUnitId,_SOURCE'
    return (
      df
      .transform(filter_null_values(columns = pk_columns))
      .transform(attach_surrogate_key(columns = pk_columns))
      .transform(attach_key('createdBy,_SOURCE', 'createdBy_ID', 'edm.user'))
      .transform(attach_key('modifiedBy,_SOURCE', 'modifiedBy_ID', 'edm.user'))
      .transform(attach_key('billToAddressId,_SOURCE', 'billToAddress_ID', 'edm.customer_location'))
      .transform(attach_key('customerId,_SOURCE', 'customer_ID', 'edm.account'))
      .transform(attach_surrogate_key(columns = 'owningBusinessUnitId,_SOURCE', name = 'owningBusinessUnit_ID'))
      .transform(attach_surrogate_key(columns = 'partyId,_SOURCE', name = 'party_ID'))
      .transform(attach_key('shipToAddressId,_SOURCE','shipToAddress_ID','edm.customer_location'))
      .transform(attach_key('soldToAddressId,_SOURCE','soldToAddress_ID','edm.customer_location'))
      .transform(attach_surrogate_key(columns = 'PayerAddressId,_SOURCE', name = 'PayerAddress_ID'))
      .transform(attach_key('salesOrganizationID,_SOURCE', 'salesOrganization_ID', 'edm.salesorganization'))
      .transform(attach_surrogate_key(columns = 'territoryId,_SOURCE', name='territory_ID'))
      .transform(attach_surrogate_key(columns = 'transactionId,invoiceType,_SOURCE', name='transaction_Id'))
      .transform(attach_key('customerId, customerDivision, salesOrganization, distributionChannel, _SOURCE', 'customerOrganization_ID', 'edm.account_organization'))
    )
  return inner

def tg_supplychain_sales_invoice_lines():
  def inner(df):
    pk_columns = 'invoiceDetailId,_SOURCE'
    return (
      df
      .transform(filter_null_values(columns = pk_columns))
      .transform(attach_surrogate_key(columns = pk_columns))
      .transform(attach_key('createdBy,_SOURCE', 'createdBy_ID', 'edm.user'))
      .transform(attach_key('modifiedBy,_SOURCE', 'modifiedBy_ID', 'edm.user'))
      .transform(attach_surrogate_key(columns = 'inventoryWarehouseId,_SOURCE', name='inventoryWarehouse_ID'))
      .transform(attach_surrogate_key(columns = 'itemId,_SOURCE', name='item_ID'))
      .transform(attach_surrogate_key(columns = 'invoiceId,owningBusinessUnitId,_SOURCE', name='invoice_ID')) 
      .transform(attach_surrogate_key(columns = 'salesOrderDetailId,_SOURCE', name='salesOrderDetail_ID'))
      .transform(attach_surrogate_key(columns = 'owningBusinessUnitId,_SOURCE', name='owningBusinessUnit_ID'))
      .transform(attach_key('shipToAddressId,_SOURCE','shipToAddress_ID','edm.customer_location'))
      .transform(attach_surrogate_key(columns = 'ordertypeId,orderType,_SOURCE', name='orderType_ID'))
    )
  return inner

def tg_supplychain_sales_order_headers():
  def inner(df):
    pk_columns = 'salesOrderId,_SOURCE'
    return (
      df
      .transform(filter_null_values(columns = pk_columns))
      .transform(attach_surrogate_key(columns = pk_columns))
      .transform(attach_key('createdBy,_SOURCE', 'createdBy_ID', 'edm.user'))
      .transform(attach_key('modifiedBy,_SOURCE', 'modifiedBy_ID', 'edm.user'))
      .transform(attach_key('billToAddressId,_SOURCE', 'billToAddress_ID', 'edm.customer_location'))
      .transform(attach_key('customerId,_SOURCE', 'customer_ID', 'edm.account'))
      .transform(attach_surrogate_key(columns = 'legalEntityID,_SOURCE', name='legalEntity_ID'))
      .transform(attach_surrogate_key(columns = 'owningBusinessUnitId,_SOURCE', name='owningBusinessUnit_ID'))
      .transform(attach_surrogate_key(columns = 'partyId,_SOURCE', name = 'party_ID'))
      .transform(attach_surrogate_key(columns = 'paymentTermId,_SOURCE', name = 'paymentTerm_ID'))
      .transform(attach_surrogate_key(columns = 'paymentMethodId,_SOURCE', name = 'paymentMethod_ID'))
      .transform(attach_surrogate_key(columns = 'territoryId,_SOURCE', name='territory_ID'))
      .transform(attach_key('shipToAddressId,_SOURCE','shipToAddress_ID','edm.customer_location'))
      .transform(attach_surrogate_key(columns = 'ordertypeId,orderType,_SOURCE', name='ordertype_ID'))
      .transform(attach_key('orderHoldBy1Id,_SOURCE', 'orderHoldBy1_ID', 'edm.user'))
      .transform(attach_key('orderHoldBy2Id,_SOURCE', 'orderHoldBy2_ID', 'edm.user'))
      .transform(attach_key('soldToAddressId,_SOURCE','soldToAddress_ID','edm.customer_location'))
      .transform(attach_surrogate_key(columns = 'PayerAddressId,_SOURCE', name='PayerAddress_ID'))
      .transform(attach_key( 'customerId, customerDivision, salesOrganization, distributionChannel, _SOURCE', 'customerOrganization_ID','edm.account_organization'))
      .transform(attach_key('salesOrganizationID,_SOURCE', 'salesOrganization_ID', 'edm.salesorganization'))      
    )
  return inner

def tg_supplychain_sales_order_line_cancellations():
  def inner(df):
    pk_columns = 'cancellationID,_SOURCE'
    return (
      df
      .transform(filter_null_values(columns = pk_columns))
      .transform(attach_surrogate_key(columns = pk_columns)) 
      .transform(attach_surrogate_key(columns = 'inventoryWarehouseID,_SOURCE', name='inventoryWarehouse_ID'))
      .transform(attach_surrogate_key(columns = 'itemId,_SOURCE', name='item_ID'))
      .transform(attach_surrogate_key(columns = 'owningBusinessUnitId,_SOURCE', name='owningBusinessUnit_ID'))
      .transform(attach_surrogate_key(columns = 'salesOrderId,_SOURCE', name='salesOrder_ID'))
      .transform(attach_surrogate_key(columns = 'salesOrderDetailId,_SOURCE', name='salesOrderDetail_ID'))
      .transform(attach_key('createdBy,_SOURCE', 'createdBy_ID', 'edm.user'))
      .transform(attach_key('modifiedBy,_SOURCE', 'modifiedBy_ID', 'edm.user'))
    )
  return inner

def tg_supplychain_sales_order_lines():
  def inner(df):
    pk_columns = 'salesOrderDetailId,_SOURCE'
    return (
      df
      .transform(filter_null_values(columns = pk_columns))
      .transform(attach_surrogate_key(columns = pk_columns)) 
      .transform(attach_surrogate_key(columns = 'inventoryWarehouseID,_SOURCE', name='inventoryWarehouse_ID'))
      .transform(attach_surrogate_key(columns = 'itemId,_SOURCE', name='item_ID'))
      .transform(attach_surrogate_key(columns = 'salesOrderId,_SOURCE', name='salesOrder_ID'))
      .transform(attach_key('shipToAddressId,_SOURCE','shipToAddress_ID','edm.customer_location'))
      .transform(attach_key('orderLineHoldBy1Id,_SOURCE', 'orderLineHoldBy1_ID', 'edm.user'))
      .transform(attach_key('orderLineHoldBy2Id,_SOURCE', 'orderLineHoldBy2_ID', 'edm.user'))
      .transform(attach_key('createdBy,_SOURCE', 'createdBy_ID', 'edm.user'))
      .transform(attach_key('modifiedBy,_SOURCE', 'modifiedBy_ID', 'edm.user'))
    )
  return inner

def tg_supplychain_sales_schedule_lines():
  def inner(df):
    pk_columns = 'salesScheduleLineId,_SOURCE'
    return (
      df
      .transform(filter_null_values(columns = pk_columns))
      .transform(attach_primary_key(pk_columns, 'edm.sales_schedule_lines'))
      .transform(attach_surrogate_key(columns = 'salesOrderId,_SOURCE', name='salesOrder_ID'))
      .transform(attach_surrogate_key(columns = 'salesOrderDetailId,_SOURCE', name = 'salesOrderDetail_ID'))
      .transform(attach_key('createdBy,_SOURCE', 'createdBy_ID', 'edm.user'))
      .transform(attach_key('modifiedBy,_SOURCE', 'modifiedBy_ID', 'edm.user'))
    )
  return inner

def tg_supplychain_sales_shipping_lines():
  def inner(df):
    pk_columns = 'deliveryDetailId,_SOURCE'
    return (
      df
      .transform(filter_null_values(columns = pk_columns))
      .transform(attach_primary_key(pk_columns, 'edm.sales_shipping_lines'))
      .transform(attach_surrogate_key(columns = 'inventoryWarehouseID,_SOURCE', name='inventoryWarehouse_ID'))
      .transform(attach_surrogate_key(columns = 'itemId,_SOURCE', name='item_ID'))
      .transform(attach_surrogate_key(columns = 'salesOrderId,_SOURCE', name='salesOrder_ID'))
      .transform(attach_surrogate_key(columns = 'salesOrderDetailId,_SOURCE', name = 'salesOrderDetail_ID'))
      .transform(attach_key('createdBy,_SOURCE', 'createdBy_ID', 'edm.user'))
      .transform(attach_key('modifiedBy,_SOURCE', 'modifiedBy_ID', 'edm.user'))
    )
  return inner


def tg_supplychain_intransit_headers():
  def inner(df):
    pk_columns = 'purchaseOrderId,_SOURCE'
    return (
      df
      .transform(filter_null_values(columns = pk_columns))
      .transform(attach_surrogate_key(columns = pk_columns)) 
      .transform(attach_key('createdBy,_SOURCE', 'createdBy_ID', 'edm.user'))
      .transform(attach_key('modifiedBy,_SOURCE', 'modifiedBy_ID', 'edm.user'))
      .transform(attach_surrogate_key(columns = 'owningBusinessUnitId,_SOURCE', name='owningBusinessUnit_ID'))
      .transform(attach_surrogate_key(columns = 'supplierId,_SOURCE', name = "supplier_ID"))
    )
  return inner

def tg_supplychain_intransit_lines():
  def inner(df):
    pk_columns = 'purchaseOrderDetailId,_SOURCE'
    return (
      df
      .transform(filter_null_values(columns = pk_columns))
      .transform(attach_surrogate_key(columns = pk_columns))
      .transform(attach_key('createdBy,_SOURCE', 'createdBy_ID', 'edm.user'))
      .transform(attach_key('modifiedBy,_SOURCE', 'modifiedBy_ID', 'edm.user'))
      .transform(attach_surrogate_key(columns = 'inventoryWarehouseId,_SOURCE', name = "inventoryWarehouse_ID"))
      .transform(attach_surrogate_key(columns = 'originatingWarehouseId,_SOURCE', name = "originatingWarehouse_ID"))
      .transform(attach_surrogate_key(columns = 'itemId,_SOURCE', name = "item_ID")) 
      .transform(attach_surrogate_key(columns = 'purchaseOrderId,_SOURCE', name = "purchaseOrder_ID"))
    )
  return inner

def tg_trade_management_point_of_sales():
  def inner(df):
    pk_columns = 'processingType, batchType, claimId, claimNumber,territoryId, userid, vertical,distributorSubmittedEndUserZipCode, dateInvoiced, claimedUom, itemId,  distributorSubmittedEndUserState, distributorSubmittedEnduserCity,distributorSubmittedEndUserName,distributorSubmittedItemNumber, marketLevel1, marketLevel2, marketLevel3,posSource,ansellContractPrice,_SOURCE'
    return (
      df
      .transform(filter_null_values(columns = pk_columns))
      .transform(attach_surrogate_key(columns = pk_columns))
      .transform(attach_key('createdBy,_SOURCE', 'createdBy_ID', 'edm.user'))
      .transform(attach_key('modifiedBy,_SOURCE', 'modifiedBy_ID', 'edm.user'))
      .transform(attach_key('accountId,_SOURCE', 'account_ID', 'edm.account'))
      .transform(attach_surrogate_key(columns = 'dateInvoiced', name='dateInvoiced_ID'))
      .transform(attach_surrogate_key(columns = 'itemId,_SOURCE', name='item_ID'))
      .transform(attach_surrogate_key(columns = 'owningBusinessUnitId,_SOURCE', name='owningBusinessUnit_ID'))
      .transform(attach_surrogate_key(columns = 'distributorPartyId,_SOURCE', name='distributorParty_ID'))
      .transform(attach_surrogate_key(columns = 'endUserPartyId,_SOURCE', name='endUserParty_ID'))
      .transform(attach_surrogate_key(columns = 'reportDate', name='reportDate_ID'))
    )
  return inner


def tg_trade_management_point_of_sales_sandel():
  def inner(df):
    pk_columns = 'skuid,city,postalcode,enduser,distributorid,_SOURCE'
    return (
      df
      .transform(filter_null_values(columns = pk_columns))
      .transform(attach_surrogate_key(columns = pk_columns))
      .transform(attach_key('createdBy,_SOURCE', 'createdBy_ID', 'edm.user'))
      .transform(attach_key('modifiedBy,_SOURCE', 'modifiedBy_ID', 'edm.user'))
      .transform(sort_columns)
    )
  return inner


def tg_trade_management_trade_promotion_accruals():
  def inner(df):
    pk_columns = 'utilizationId,_SOURCE'
    return (
      df
      .transform(filter_null_values(columns = pk_columns))
      .transform(attach_surrogate_key(columns = pk_columns))
      .transform(attach_key('createdBy,_SOURCE', 'createdBy_ID', 'edm.user'))
      .transform(attach_key('modifiedBy,_SOURCE', 'modifiedBy_ID', 'edm.user'))
      .transform(attach_key('billToAddressId,_SOURCE', 'billToAddress_ID', 'edm.customer_location'))
      .transform(attach_key('accountId,_SOURCE', 'account_ID', 'edm.account'))
      .transform(attach_surrogate_key(columns = 'invoiceId,_SOURCE', name='invoice_ID'))
      .transform(attach_surrogate_key(columns = 'itemId,_SOURCE', name='item_ID'))
      .transform(attach_surrogate_key(columns = 'owningBusinessUnitId,_SOURCE', name='owningBusinessUnit_ID'))
      .transform(attach_surrogate_key(columns = 'salesorderDetailId,_SOURCE', name='salesorderDetail_ID'))
      .transform(attach_key('shipToAddressId,_SOURCE','shipToAddress_ID','edm.customer_location'))
      .transform(sort_columns)
    )
  return inner


def tg_trade_management_price_list_header():
  def inner(df):
    pk_columns = 'priceListHeaderId,_SOURCE'
    return (
      df
      .transform(filter_null_values(columns = pk_columns))
      .transform(attach_primary_key(pk_columns, 'edm.price_list_header'))
      .transform(attach_key('createdBy,_SOURCE', 'createdBy_ID', 'edm.user'))
      .transform(attach_key('modifiedBy,_SOURCE', 'modifiedBy_ID', 'edm.user'))
    )
  return inner



def tg_trade_management_price_list_line():
  def inner(df):
    pk_columns = 'priceListLineId,_SOURCE'
    return (
      df
      .transform(filter_null_values(columns = pk_columns))
      .transform(attach_primary_key(pk_columns, 'edm.price_list_line'))
      .transform(attach_key('createdBy,_SOURCE', 'createdBy_ID', 'edm.user'))
      .transform(attach_key('modifiedBy,_SOURCE', 'modifiedBy_ID', 'edm.user'))
      .transform(attach_key('priceListHeaderId,_SOURCE', 'priceListHeader_ID', 'edm.price_list_header'))
      .transform(attach_surrogate_key(columns = 'priceListItemId,_SOURCE', name='priceListItem_ID'))
    )
  return inner


def tg_trade_management_price_list_qualifier():
  def inner(df):
    pk_columns = 'qualifierId,_SOURCE'
    return (
      df
      .transform(filter_null_values(columns = pk_columns))
      .transform(attach_primary_key(pk_columns, 'edm.price_list_qualifier'))
      .transform(attach_key('createdBy,_SOURCE', 'createdBy_ID', 'edm.user'))
      .transform(attach_key('modifiedBy,_SOURCE', 'modifiedBy_ID', 'edm.user'))
      .transform(attach_key('priceListHeaderId,_SOURCE', 'priceListHeader_ID', 'edm.price_list_header'))
    )
  return inner
