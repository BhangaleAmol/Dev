-- Databricks notebook source
select 
'lom_aa_do_l1',
count(*)
from 
  g_tembo.lom_aa_do_l1

-- COMMAND ----------

select 
'lom_aa_do_l1_hist',
count(*)
from 
  g_tembo.lom_aa_do_l1_hist

-- COMMAND ----------

select 
'lom_aa_do_l4',
count(*)
from 
  g_tembo.lom_aa_do_l4

-- COMMAND ----------

select 
'lom_aa_do_l4_hist',
count(*)
from 
  g_tembo.lom_aa_do_l4_hist

-- COMMAND ----------

select 
'lom_au_do_l1',
count(*)
from 
  g_tembo.lom_au_do_l1

-- COMMAND ----------

select 
'lom_au_do_l1_hist',
count(*)
from 
  g_tembo.lom_au_do_l1_hist

-- COMMAND ----------

select 
'lom_hh_do_l1_hist' interface,
year, period, count(*), sum(demand), sum(Historical_ADS1_data), sum(Value_history_in_currency), sum(Historical_ADS2_data)
from g_tembo.lom_hh_do_l1_hist
group by year, period
order by 2,3

-- COMMAND ----------

select 
'lom_hh_do_l1' interface,
year, period, count(*),  sum(Historical_ADS1_data), sum(Value_history_in_currency), sum(Historical_ADS2_data)
from g_tembo.lom_hh_do_l1
group by year, period
order by 2,3

-- COMMAND ----------

select 
'lom_xx_do_l1' interface,
year, period, sum(user_array_1), sum(user_array_2), sum(user_array_8),
  count(*) count
from g_tembo.lom_xx_do_l1
group by 
year, period
order by 2,3

-- COMMAND ----------

select 
'lom_xx_do_l1_hist' interface,
year, period, sum(user_array_1), sum(user_array_2), sum(user_array_8),
  count(*) count
from g_tembo.lom_xx_do_l1_hist
group by 
year, period
order by 2,3

-- COMMAND ----------

select 
'lom_ff_do_l1' interface,
  year,
  period,
  sum(future_order),
  count(*) count
from 
  g_tembo.lom_ff_do_l1
group by 
  year,
  period
order by 2,3
