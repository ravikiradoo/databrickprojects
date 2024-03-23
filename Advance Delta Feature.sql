-- Databricks notebook source
create catalog if not exists salesext managed location 'abfss://databrickcontainer@datalakedatadrick1302.dfs.core.windows.net/';
use catalog salesext;
create schema if not exists sales;
use schema  sales;
create or replace table products 
(
ProductID int ,
ProductName varchar(100),
Category   varchar(100),
ListPrice  float
 );

-- COMMAND ----------

insert into products values(1,'test','test',200);

-- COMMAND ----------

describe detail products;

-- COMMAND ----------

insert into products 
values (2,'test2','test3',200),
 (3,'test2','test3',200),
  (4,'test2','test3',200),
   (5,'test2','test3',200),
    (6,'test2','test3',200);

-- COMMAND ----------

insert into products values (7,'test2','test3',200);
insert into products values(9,'test2','test3',200);

-- COMMAND ----------

update products set productName ='test9' where productId=9;

-- COMMAND ----------

use catalog salesext ;
use schema sales;
select * from products version as of 5;

-- COMMAND ----------

delete from products;

-- COMMAND ----------

select * from products;

-- COMMAND ----------

restore table products to version as of 5;

-- COMMAND ----------

select * from products;

-- COMMAND ----------

optimize products zorder by (ProductID);

-- COMMAND ----------

describe detail products;

-- COMMAND ----------

describe history products;

-- COMMAND ----------

set spark.databricks.delta.retentionDurationCheck.enabled=false;

-- COMMAND ----------

vacuum products retain 0 hours;

-- COMMAND ----------

select * from products;
