-- Databricks notebook source
--Read data from CSV file directly
select * from CSV.`/FileStore/tables/csv/products.csv`;

-- COMMAND ----------

drop table if exists product_direct;
create  table product_direct 
( 
ProductID int  ,
ProductName varchar(100),
Category   varchar(100),
ListPrice  float
)
using CSV
options(header=True)
location 'abfss://databrickcontainer@datalakedatadrick1302.dfs.core.windows.net/databrickws/products/products.csv';

-- COMMAND ----------

select * from product_direct;

-- COMMAND ----------

--clear cache
REFRESH TABLE Product_direct;

-- COMMAND ----------

create or replace table products_csv as
select * from CSV.`/FileStore/tables/csv/`;

-- COMMAND ----------

select * from products_csv;

-- COMMAND ----------

insert overwrite products_csv
select * from CSV.`/FileStore/tables/csv/`;

-- COMMAND ----------

insert into products_csv
select * from CSV.`/FileStore/tables/csv/`;
