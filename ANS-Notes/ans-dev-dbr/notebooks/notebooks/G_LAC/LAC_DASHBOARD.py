# Databricks notebook source
# MAGIC %sql
# MAGIC create database if not exists g_lac

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW g_lac.lac_sales_orders_v AS
# MAGIC SELECT
# MAGIC   order_lines._SOURCE,
# MAGIC   order_lines.item_id,
# MAGIC   order_headers.owningBusinessUnit_ID,
# MAGIC   order_lines.inventoryWarehouse_ID,
# MAGIC   order_headers.customer_Id,
# MAGIC   order_headers.territory_id,
# MAGIC   order_headers.orderDate,
# MAGIC   order_lines.requestDeliveryBy,
# MAGIC   order_headers.orderNumber,
# MAGIC   order_headers.baseCurrencyId,
# MAGIC   order_headers.transactionCurrencyId,
# MAGIC   order_lines.orderUomCode,
# MAGIC   order_lines.productCode,
# MAGIC   order_headers.exchangeRate,
# MAGIC   order_headers.exchangeRateUsd,
# MAGIC   nvl(order_lines.ansStdUomConv,1) ansStdUomConv,
# MAGIC   nvl(product.ansStdUom,1) ansStdUom,
# MAGIC   round(sum(order_lines.quantityOrdered), 0) quantityOrdered,
# MAGIC   round(sum(order_lines.orderAmount), 2) orderValueTransactionCurrency,
# MAGIC   round(sum(futureOrderValue),2) futureOrderValue,
# MAGIC   round(sum(outstandingOrderValue),2) outstandingOrderValue,
# MAGIC   round(sum(quantityFutureOrders),2) quantityFutureOrders,
# MAGIC   round(sum(quantityOutstandingOrders),2) quantityOutstandingOrders,
# MAGIC   round(sum(quantityBackOrdered),2) quantityBackOrdered ,
# MAGIC   round(sum(backOrderValue),2) backOrderValue,
# MAGIC   order_headers.billtoAddress_ID,
# MAGIC   order_headers.orderType_ID
# MAGIC from
# MAGIC   s_supplychain.sales_order_headers_agg order_headers
# MAGIC   inner join s_supplychain.sales_order_lines_agg order_lines on order_headers._ID = order_lines.salesOrder_ID
# MAGIC   inner join s_core.organization_agg organization on order_headers.owningBusinessUnit_ID = organization._ID
# MAGIC   inner join s_core.product_agg product on order_lines.item_id = product._Id
# MAGIC   left join s_core.account_agg accounts on order_headers.customer_id = accounts._ID
# MAGIC   left join s_core.customer_location_agg ship_to on order_lines.shipToAddress_ID = ship_to._ID
# MAGIC where
# MAGIC   not order_lines._DELETED
# MAGIC   and not order_headers._DELETED
# MAGIC   and not organization._DELETED
# MAGIC   and not product._DELETED
# MAGIC   and not accounts._DELETED
# MAGIC   and not ship_to._DELETED
# MAGIC   and  order_headers._SOURCE in ('EBS', 'TOT', 'COL')
# MAGIC   and year(order_headers.orderDate) >= year(current_date) - 2
# MAGIC   and (organization.organizationCode in ('5220', '5210', '5400', '5100', '7473')
# MAGIC     or nvl(ship_to.SiteCategory, 'NA') in ('unknown', 'LA'))
# MAGIC   and organization.organizationId not in ('2190')  -- exclude SW
# MAGIC   and accounts.customerType = 'External' -- exclude Internal orders
# MAGIC group by
# MAGIC   order_lines._SOURCE,
# MAGIC   order_headers.orderDate,
# MAGIC   order_lines.requestDeliveryBy,
# MAGIC   order_headers.orderNumber,
# MAGIC   order_headers.baseCurrencyId,
# MAGIC   order_headers.transactionCurrencyId,
# MAGIC   order_lines.orderUomCode,
# MAGIC   order_lines.productCode,
# MAGIC   order_lines.item_id,
# MAGIC   order_headers.owningBusinessUnit_ID,
# MAGIC   order_lines.inventoryWarehouse_ID,
# MAGIC   order_headers.customer_Id,
# MAGIC   order_headers.exchangeRate,
# MAGIC   order_headers.exchangeRateUsd,
# MAGIC   order_headers.territory_id,
# MAGIC   order_lines.ansStdUomConv,
# MAGIC   product.ansStdUom,
# MAGIC   order_headers.billtoAddress_ID,
# MAGIC   order_headers.orderType_ID

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace view g_lac.lac_order_types_v AS
# MAGIC select
# MAGIC _ID,
# MAGIC description,
# MAGIC dropShipmentFlag,
# MAGIC transactionTypeCode
# MAGIC from s_core.transaction_type_agg
# MAGIC where not _deleted
# MAGIC and transactionGroup = 'ORDER_TYPE'
# MAGIC and _SOURCE in ('EBS', 'TOT', 'COL') 

# COMMAND ----------

# MAGIC %sql 
# MAGIC CREATE OR REPLACE VIEW g_lac.lac_sales_invoices_v AS
# MAGIC SELECT
# MAGIC   invoice_headers._SOURCE,
# MAGIC   invoice_lines.item_id,
# MAGIC   invoice_headers.owningBusinessUnit_ID,
# MAGIC   invoice_lines.inventoryWarehouse_ID,
# MAGIC   invoice_headers.customer_Id,
# MAGIC   invoice_headers.territory_Id,
# MAGIC   invoice_headers.dateInvoiced,
# MAGIC   invoice_headers.invoiceNumber,
# MAGIC   invoice_headers.baseCurrencyId,
# MAGIC   invoice_headers.transactionCurrencyId,
# MAGIC   invoice_lines.orderUomCode,
# MAGIC   invoice_lines.productCode,
# MAGIC   invoice_headers.exchangeRate,
# MAGIC   invoice_headers.exchangeRateUsd,
# MAGIC   sum(invoice_lines.quantityInvoiced) quantityInvoiced,
# MAGIC   sum(invoice_lines.baseAmount) baseAmountDocCurr,
# MAGIC   sum(invoice_lines.seeThruCost) seeThruCost,
# MAGIC   invoice_headers.billtoAddress_ID
# MAGIC from
# MAGIC   s_supplychain.sales_invoice_headers_agg invoice_headers
# MAGIC   inner join s_supplychain.sales_invoice_lines_agg invoice_lines on invoice_headers._ID = invoice_lines.invoice_ID
# MAGIC   inner join s_core.organization_agg organization on invoice_headers.owningBusinessUnit_ID = organization._ID
# MAGIC   left outer join s_core.account_agg accounts on invoice_headers.customer_id = accounts._ID
# MAGIC   left join s_core.customer_location_agg ship_to on invoice_headers.shipToAddress_ID = ship_to._ID
# MAGIC where
# MAGIC   not invoice_headers._DELETED
# MAGIC   and not invoice_lines._DELETED
# MAGIC   and not organization._DELETED
# MAGIC   and not accounts._DELETED
# MAGIC   and not ship_to._DELETED
# MAGIC   and invoice_headers._SOURCE in ('EBS', 'TOT', 'COL')
# MAGIC   and year(invoice_headers.dateInvoiced) >= year(current_date) - 2
# MAGIC   and (organization.organizationCode in ('5220', '5210', '5400', '5100', '7473')
# MAGIC   or nvl(ship_to.SiteCategory, 'NA') in ('unknown', 'LA'))
# MAGIC   and organization.organizationCode not in ('2190')  -- exclude SW
# MAGIC   and accounts.customerType = 'External' -- exclude Internal orders
# MAGIC group by
# MAGIC   invoice_headers._SOURCE,
# MAGIC   invoice_lines.item_id,
# MAGIC   invoice_headers.owningBusinessUnit_ID,
# MAGIC   invoice_lines.inventoryWarehouse_ID,
# MAGIC   invoice_headers.customer_Id,
# MAGIC   invoice_headers.dateInvoiced,
# MAGIC   invoice_headers.invoiceNumber,
# MAGIC   invoice_headers.baseCurrencyId,
# MAGIC   invoice_headers.transactionCurrencyId,
# MAGIC   invoice_lines.orderUomCode,
# MAGIC   invoice_lines.productCode,
# MAGIC   invoice_headers.exchangeRate,
# MAGIC   invoice_headers.exchangeRateUsd,
# MAGIC   invoice_headers.territory_Id,
# MAGIC   invoice_headers.billtoAddress_ID

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW g_lac.lac_product_v AS
# MAGIC select
# MAGIC   _ID,
# MAGIC   _SOURCE,
# MAGIC   gbu,
# MAGIC   itemId,
# MAGIC   name,
# MAGIC   productStyle,
# MAGIC   productStyleDescription,
# MAGIC   productSbu,
# MAGIC   productCode
# MAGIC from
# MAGIC   s_core.product_agg
# MAGIC where not _deleted 
# MAGIC   and _source in ('EBS', 'TOT', 'COL')
# MAGIC    

# COMMAND ----------

# MAGIC %sql 
# MAGIC CREATE OR REPLACE VIEW g_lac.lac_organization_v AS
# MAGIC select
# MAGIC   _ID,
# MAGIC   _SOURCE,
# MAGIC   organizationId,
# MAGIC   organizationCode,
# MAGIC   name,
# MAGIC   organizationType
# MAGIC from
# MAGIC   s_core.organization_agg c
# MAGIC where not _deleted
# MAGIC   and _source in ('TOT', 'COL','EBS')
# MAGIC    

# COMMAND ----------

# MAGIC %sql 
# MAGIC CREATE OR REPLACE VIEW g_lac.lac_account_v AS
# MAGIC select
# MAGIC   _ID,
# MAGIC   _SOURCE,
# MAGIC   accountId,
# MAGIC   accountNumber,
# MAGIC   name,
# MAGIC   address1Country,
# MAGIC   businessGroup_ID
# MAGIC from
# MAGIC   s_core.account_agg
# MAGIC where not _deleted
# MAGIC   and _source in ('EBS', 'TOT', 'COL')
# MAGIC    

# COMMAND ----------

# MAGIC %sql 
# MAGIC CREATE OR REPLACE VIEW g_lac.lac_business_groups_v AS
# MAGIC select
# MAGIC   _ID,
# MAGIC   _SOURCE,
# MAGIC   groupId,
# MAGIC   name
# MAGIC from
# MAGIC   s_core.groupings_agg
# MAGIC where not _deleted
# MAGIC   and _source in ('EBS', 'TOT', 'COL')
# MAGIC   and groupType in ('BusinessGroup')
# MAGIC    

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace view g_lac.lac_budgets_v AS
# MAGIC select
# MAGIC   organization_agg._ID,
# MAGIC   organization_agg.organizationCode,
# MAGIC   sales_budget_agg.budgetMonth,
# MAGIC   sales_budget_agg.legalEntityExchangeRate,
# MAGIC   sales_budget_agg.costExchangeRate,
# MAGIC   sales_budget_agg.salesExchangeRate,
# MAGIC   legalEntityCurrency,
# MAGIC   budgetSalesCurrency,
# MAGIC   budgetCostCurrency,
# MAGIC   budgetProductCode,
# MAGIC   item_ID,
# MAGIC   sum(sales_budget_agg.costBudget) costBudget,
# MAGIC   sum(sales_budget_agg.qtyBudget) qtyBudget,
# MAGIC   sum(sales_budget_agg.salesBudget) salesBudget,
# MAGIC   sum(case when sales_budget_agg.budgetCostCurrency = 'USD' 
# MAGIC           then sales_budget_agg.costBudget
# MAGIC           else sales_budget_agg.costBudget / sales_budget_agg.costExchangeRate
# MAGIC       end) costBudgetUsd,
# MAGIC   sum(case when sales_budget_agg.budgetSalesCurrency = 'USD' 
# MAGIC           then sales_budget_agg.salesBudget
# MAGIC           else sales_budget_agg.salesBudget / sales_budget_agg.salesExchangeRate
# MAGIC       end) salesBudgetUsd,
# MAGIC   sum(case when sales_budget_agg.budgetCostCurrency =  sales_budget_agg.legalEntityCurrency
# MAGIC           then sales_budget_agg.costBudget
# MAGIC           else sales_budget_agg.costBudget * sales_budget_agg.legalEntityExchangeRate
# MAGIC       end) costBudgetLe,
# MAGIC   sum(case when sales_budget_agg.budgetSalesCurrency = sales_budget_agg.legalEntityCurrency
# MAGIC           then sales_budget_agg.salesBudget
# MAGIC            else sales_budget_agg.salesBudget * sales_budget_agg.legalEntityExchangeRate
# MAGIC       end) salesBudgetLe,
# MAGIC   sum(case when sales_budget_agg.budgetCostCurrency = 'USD' 
# MAGIC            then sales_budget_agg.costForecast
# MAGIC            else sales_budget_agg.costForecast / sales_budget_agg.costExchangeRate
# MAGIC        end) costForecastUsd,
# MAGIC   sum(case when sales_budget_agg.budgetSalesCurrency = 'USD' 
# MAGIC            then sales_budget_agg.salesForecast
# MAGIC            else sales_budget_agg.salesForecast / sales_budget_agg.salesExchangeRate
# MAGIC        end) salesForecastUsd,
# MAGIC   sum(case when sales_budget_agg.budgetCostCurrency =  sales_budget_agg.legalEntityCurrency
# MAGIC            then sales_budget_agg.costForecast
# MAGIC            else sales_budget_agg.costForecast * sales_budget_agg.legalEntityExchangeRate
# MAGIC        end) costForecastLe,
# MAGIC   sum(case when sales_budget_agg.budgetSalesCurrency = sales_budget_agg.legalEntityCurrency
# MAGIC            then sales_budget_agg.salesForecast
# MAGIC             else sales_budget_agg.salesForecast * sales_budget_agg.legalEntityExchangeRate
# MAGIC        end) salesForecastLe
# MAGIC from s_core.sales_budget_agg,
# MAGIC s_core.organization_agg
# MAGIC where
# MAGIC  not sales_budget_agg._deleted
# MAGIC   and not organization_agg._deleted
# MAGIC   and sales_budget_agg.owningBusinessUnit_Id = organization_agg._ID
# MAGIC   and sales_budget_agg.budgetType = 'BUDGET'
# MAGIC group by
# MAGIC   organization_agg.organizationCode,
# MAGIC   sales_budget_agg.budgetMonth,
# MAGIC   sales_budget_agg.legalEntityExchangeRate,
# MAGIC   sales_budget_agg.costExchangeRate,
# MAGIC   sales_budget_agg.salesExchangeRate,
# MAGIC   legalEntityCurrency,
# MAGIC   budgetSalesCurrency,
# MAGIC   budgetCostCurrency,
# MAGIC   organization_agg._ID,
# MAGIC   budgetProductCode,
# MAGIC   item_ID

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace view g_lac.lac_sales_organization_v AS
# MAGIC select
# MAGIC _id,
# MAGIC regionManager,
# MAGIC territoryManager,
# MAGIC percentAllocated,
# MAGIC accountId,
# MAGIC account_id,
# MAGIC businessGroupId,
# MAGIC businessGroup_id
# MAGIC
# MAGIC from s_core.customer_sales_organization_agg
# MAGIC
# MAGIC where  not _deleted
# MAGIC and _source in ('EBS', 'TOT', 'COL')

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW g_lac.lac_inventory_v AS
# MAGIC select
# MAGIC   inventory_agg.ansStdUomCode,
# MAGIC   inventory_agg.primaryUomCode,
# MAGIC   inventory_agg.inventoryDate,
# MAGIC   inventory_agg.itemID,
# MAGIC   inventory_agg.item_ID,
# MAGIC   inventory_agg.inventoryWarehouse_ID,
# MAGIC   inventory_agg.owningBusinessUnit_ID,
# MAGIC   round(sum(inventory_agg.ansStdQty),0) ansStdQty,
# MAGIC   round(sum(inventory_agg.primaryQty),0) primaryQty
# MAGIC from
# MAGIC   s_supplychain.inventory_agg
# MAGIC   inner join s_core.organization_agg organization on inventory_agg.owningBusinessUnit_ID = organization._ID
# MAGIC where 
# MAGIC   not inventory_agg._DELETED
# MAGIC   and not organization._DELETED
# MAGIC   and inventory_agg._source in ('EBS', 'TOT', 'COL')
# MAGIC   and organization.organizationCode in ('5220', '5210', '5400', '5100')
# MAGIC group by
# MAGIC   ansStdUomCode,
# MAGIC   primaryUomCode,
# MAGIC   inventoryDate,
# MAGIC   itemID,
# MAGIC   item_ID,
# MAGIC   inventoryWarehouse_ID,
# MAGIC   owningBusinessUnit_ID

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace view g_lac.lac_opportunity_lines_v as
# MAGIC select
# MAGIC   opportunitylineitem.opportunityid opportunityid,
# MAGIC   opportunitylineitem.product2id as ProductId,
# MAGIC   opportunitylineitem.Subtotal AS amountBaseCurrency,
# MAGIC   opportunitylineitem.Subtotal_USD AS amountUsd,
# MAGIC   opportunitylineitem.Quantity AS quantity,
# MAGIC   opportunitylineitem.UnitPrice AS unitPrice,
# MAGIC   opportunitylineitem.UOM_New__c uom
# MAGIC from
# MAGIC   sf.opportunitylineitem
# MAGIC   join sf.opportunity on opportunitylineitem.opportunityid = opportunity.id
# MAGIC where
# MAGIC    not opportunitylineitem._DELETED
# MAGIC   and not opportunity._DELETED 
# MAGIC   and opportunity.IsWon = 0
# MAGIC   AND opportunity.IsClosed = 0
# MAGIC   and opportunity.Organisation2__c like '%America%'
# MAGIC   and opportunity.account_country__c not in ('US')

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace view g_lac.lac_opportunity_v as
# MAGIC select
# MAGIC   opportunity.createddate,
# MAGIC   opportunity.createdbyid,
# MAGIC   opportunity.lastmodifieddate,
# MAGIC   opportunity.lastmodifiedbyid,
# MAGIC   opportunity.id,
# MAGIC   opportunity.accountId,
# MAGIC   opportunity.recordtypeId,
# MAGIC   opportunity.Name,
# MAGIC   opportunity.stagename,
# MAGIC   opportunity.closedate,
# MAGIC   opportunity.type,
# MAGIC   opportunity.ownerid,
# MAGIC   opportunity.Guardian__c guardianFlag,
# MAGIC   NULL guardianType,
# MAGIC   Organisation2__c organization,
# MAGIC   opportunity.territory__c territory,
# MAGIC   opportunity.CurrencyIsoCode,
# MAGIC   opportunity.Region3__c subRegion,
# MAGIC   opportunity.Account_Country__c country,
# MAGIC   Distributor_lookup__c distributorId,
# MAGIC   CASE
# MAGIC     WHEN opportunity.IsWon = 0
# MAGIC     AND opportunity.IsClosed = 0 THEN 'Open'
# MAGIC     WHEN opportunity.IsWon = 0
# MAGIC     AND opportunity.IsClosed = 1 THEN 'Closed'
# MAGIC     WHEN opportunity.IsWon = 1
# MAGIC     AND opportunity.IsClosed = 1 THEN 'Closed Won'
# MAGIC   END AS status
# MAGIC from
# MAGIC   sf.opportunity
# MAGIC where
# MAGIC   not _deleted
# MAGIC   and opportunity.IsWon = 0
# MAGIC   AND opportunity.IsClosed = 0
# MAGIC   and opportunity.Organisation2__c like '%America%'
# MAGIC   and opportunity.account_country__c not in ('US')

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace view g_lac.lac_sf_account_v AS
# MAGIC select
# MAGIC   distinct account.id,
# MAGIC   account.name,
# MAGIC   account.billingstreet,
# MAGIC   account.billingcity,
# MAGIC   account.billingstate,
# MAGIC   account.billingpostalcode,
# MAGIC   account.billingcountry,
# MAGIC   account.billingstatecode,
# MAGIC   account.billingcountrycode,
# MAGIC   account.shippingstreet,
# MAGIC   account.shippingcity,
# MAGIC   account.shippingstate,
# MAGIC   account.shippingpostalcode,
# MAGIC   account.shippingcountry,
# MAGIC   account.shippingstatecode,
# MAGIC   account.shippingcountrycode,
# MAGIC   account.ownerid,
# MAGIC   account.sub_vertical__c subvertical,
# MAGIC   account.vertical__c vertical,
# MAGIC   account.type__c accountType,
# MAGIC   account.ParentId
# MAGIC from
# MAGIC   sf.account
# MAGIC   join sf.opportunity on opportunity.accountid = account.id
# MAGIC where
# MAGIC   not account._deleted
# MAGIC   and not opportunity._deleted
# MAGIC   and opportunity.IsWon = 0
# MAGIC   AND opportunity.IsClosed = 0
# MAGIC   and opportunity.Organisation2__c like '%America%'
# MAGIC   and opportunity.account_country__c not in ('US')

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace view g_lac.lac_sf_product_v as
# MAGIC select
# MAGIC   distinct product.id id,
# MAGIC   product.name,
# MAGIC   product.productcode,
# MAGIC   product.description,
# MAGIC   product.product_division__c productDivision,
# MAGIC   product.GBU__C gbu,
# MAGIC   product.product_SBU__c SBU,
# MAGIC   Style__c productStyle
# MAGIC from
# MAGIC   sf.product2 product
# MAGIC   join sf.opportunitylineitem on opportunitylineitem.product2id = product.id
# MAGIC   join sf.opportunity on opportunitylineitem.opportunityid = opportunity.id
# MAGIC where
# MAGIC   not product._deleted
# MAGIC   and not opportunity._deleted
# MAGIC   and not opportunitylineitem._deleted
# MAGIC   and opportunity.IsWon = 0
# MAGIC   AND opportunity.IsClosed = 0
# MAGIC   and opportunity.Organisation2__c like '%America%'
# MAGIC   and opportunity.account_country__c not in ('US')

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace view g_lac.lac_user_v as
# MAGIC select
# MAGIC   user.id,
# MAGIC   user.name,
# MAGIC   user.firstName,
# MAGIC   user.lastName,
# MAGIC   user.title,
# MAGIC   user.postalcode,
# MAGIC   user.country,
# MAGIC   user.countrycode,
# MAGIC   user.email,
# MAGIC   user.territory__c territory,
# MAGIC   user.user_region__c region
# MAGIC FROM
# MAGIC   sf.user
# MAGIC   where not _deleted

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW g_lac.lac_customer_location_v AS
# MAGIC select
# MAGIC   _ID,
# MAGIC   accountId,
# MAGIC   addressId,
# MAGIC   addressline1,
# MAGIC   addressline2,
# MAGIC   addressline3,
# MAGIC   city,
# MAGIC   country,
# MAGIC   postalCode,
# MAGIC   siteCategory,
# MAGIC   siteUseCode
# MAGIC from
# MAGIC  s_core.customer_location_agg
# MAGIC where
# MAGIC   _source in ('EBS', 'COL', 'TOT')
# MAGIC   and not _deleted

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW g_lac.lac_intransit_v AS
# MAGIC select
# MAGIC   ih.baseCurrencyId,
# MAGIC   ih.Currency,
# MAGIC   ih.orderNumber,
# MAGIC   ih.owningBusinessUnit_ID,
# MAGIC   ih.supplier_ID,
# MAGIC   il.item_ID,
# MAGIC   il.orderLineStatus,
# MAGIC   il.orderUomCode,
# MAGIC   il.inventoryWarehouseId,
# MAGIC   il.lineNumber,
# MAGIC   il.lotNumber,
# MAGIC   il.lotExpirationDate,
# MAGIC   il.needByDate,
# MAGIC   il.originatingWarehouseId,
# MAGIC   il.pricePerUnit,
# MAGIC   il.promisedDate,
# MAGIC   il.purchaseOrderId,
# MAGIC   il.quantityOnOrder,
# MAGIC   il.quantityReceived,
# MAGIC   il.quantityShipped,
# MAGIC   il.retd,
# MAGIC   il.shipDate,
# MAGIC   il.shippingQuantityUom
# MAGIC from
# MAGIC   s_supplychain.intransit_headers_agg ih
# MAGIC   join s_supplychain.intransit_lines_agg il on il.purchaseOrder_ID = ih._ID
# MAGIC where
# MAGIC   not il._deleted
# MAGIC   and not ih._deleted
# MAGIC   and ih._source in ('TOT', 'COL', 'EBS')

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW g_lac.lac_supplier_v AS
# MAGIC select
# MAGIC   supplierId,
# MAGIC   supplierName,
# MAGIC   supplierNumber,
# MAGIC   _ID
# MAGIC from
# MAGIC   s_core.supplier_account_agg
# MAGIC where
# MAGIC   not _deleted
# MAGIC   and _source in ('TOT', 'COL', 'EBS')

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW g_lac.lac_customer_sales_organization_v AS
# MAGIC select
# MAGIC   _ID,
# MAGIC   _SOURCE,
# MAGIC   accountId,
# MAGIC   businessGroup_ID
# MAGIC   percentAllocated,
# MAGIC   regionManager,
# MAGIC   territoryManager
# MAGIC from
# MAGIC   s_core.customer_sales_organization_agg
# MAGIC where
# MAGIC   _source in ('EBS', 'TOT', 'COL')
# MAGIC   and not _deleted

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace view g_lac.lac_forecast_v AS
# MAGIC select
# MAGIC   account_ID,
# MAGIC   organization_agg._ID,
# MAGIC   organization_agg.organizationCode,
# MAGIC   sales_forecast_agg.budgetMonth,
# MAGIC   sales_forecast_agg.legalEntityExchangeRate,
# MAGIC   sales_forecast_agg.costExchangeRate,
# MAGIC   sales_forecast_agg.salesExchangeRate,
# MAGIC   legalEntityCurrency,
# MAGIC   budgetSalesCurrency,
# MAGIC   budgetCostCurrency,
# MAGIC   budgetProductCode,
# MAGIC   item_ID,
# MAGIC   sum(sales_forecast_agg.costForecast) costForecast,
# MAGIC   sum(sales_forecast_agg.qtyForecast) qtyForecast,
# MAGIC   sum(sales_forecast_agg.salesForecast) salesForecast,
# MAGIC   sum(case when sales_forecast_agg.budgetCostCurrency = 'USD'
# MAGIC           then sales_forecast_agg.costBudget
# MAGIC           else sales_forecast_agg.costBudget / sales_forecast_agg.costExchangeRate
# MAGIC       end) costBudgetUsd,
# MAGIC   sum(case when sales_forecast_agg.budgetSalesCurrency = 'USD'
# MAGIC           then sales_forecast_agg.salesBudget
# MAGIC           else sales_forecast_agg.salesBudget / sales_forecast_agg.salesExchangeRate
# MAGIC       end) salesBudgetUsd,
# MAGIC   sum(case when sales_forecast_agg.budgetCostCurrency =  sales_forecast_agg.legalEntityCurrency
# MAGIC           then sales_forecast_agg.costBudget
# MAGIC           else sales_forecast_agg.costBudget * sales_forecast_agg.legalEntityExchangeRate
# MAGIC       end) costBudgetLe,
# MAGIC   sum(case when sales_forecast_agg.budgetSalesCurrency = sales_forecast_agg.legalEntityCurrency
# MAGIC           then sales_forecast_agg.salesBudget
# MAGIC            else sales_forecast_agg.salesBudget * sales_forecast_agg.legalEntityExchangeRate
# MAGIC       end) salesBudgetLe,
# MAGIC   sum(case when sales_forecast_agg.budgetCostCurrency = 'USD'
# MAGIC            then sales_forecast_agg.costForecast
# MAGIC            else sales_forecast_agg.costForecast / sales_forecast_agg.costExchangeRate
# MAGIC        end) costForecastUsd,
# MAGIC   sum(case when sales_forecast_agg.budgetSalesCurrency = 'USD'
# MAGIC            then sales_forecast_agg.salesForecast
# MAGIC            else sales_forecast_agg.salesForecast / sales_forecast_agg.salesExchangeRate
# MAGIC        end) salesForecastUsd,
# MAGIC   sum(case when sales_forecast_agg.budgetCostCurrency =  sales_forecast_agg.legalEntityCurrency
# MAGIC            then sales_forecast_agg.costForecast
# MAGIC            else sales_forecast_agg.costForecast * sales_forecast_agg.legalEntityExchangeRate
# MAGIC        end) costForecastLe,
# MAGIC   sum(case when sales_forecast_agg.budgetSalesCurrency = sales_forecast_agg.legalEntityCurrency
# MAGIC            then sales_forecast_agg.salesForecast
# MAGIC             else sales_forecast_agg.salesForecast * sales_forecast_agg.legalEntityExchangeRate
# MAGIC        end) salesForecastLe
# MAGIC from s_core.sales_forecast_agg,
# MAGIC   s_core.organization_agg
# MAGIC where
# MAGIC   not sales_forecast_agg._deleted
# MAGIC   and not organization_agg._deleted
# MAGIC   and sales_forecast_agg.owningBusinessUnit_Id = organization_agg._ID
# MAGIC   and sales_forecast_agg.budgetType = 'FORECAST'
# MAGIC group by
# MAGIC   account_ID,
# MAGIC   organization_agg.organizationCode,
# MAGIC   sales_forecast_agg.budgetMonth,
# MAGIC   sales_forecast_agg.legalEntityExchangeRate,
# MAGIC   sales_forecast_agg.costExchangeRate,
# MAGIC   sales_forecast_agg.salesExchangeRate,
# MAGIC   legalEntityCurrency,
# MAGIC   budgetSalesCurrency,
# MAGIC   budgetCostCurrency,
# MAGIC   organization_agg._ID,
# MAGIC   budgetProductCode,
# MAGIC   item_ID
