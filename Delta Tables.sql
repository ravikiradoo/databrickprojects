-- Databricks notebook source
create table databrickwsnew.extschema.product_ext
(
  ProductId int,
  productName varchar(100),
  unitPrice float
);

-- COMMAND ----------

describe detail databrickwsnew.extschema.product_ext;

-- COMMAND ----------

create table product_new 
comment 'migration table'
partitioned by (ProductId)
select * from databrickwsnew.extschema.product_ext;

-- COMMAND ----------

alter table product_new add constraint check_unitprice check (unitPrice >0);

-- COMMAND ----------

create table product_clone deep clone product_new;

-- COMMAND ----------

select * from product_clone;

-- COMMAND ----------

insert into product_new values(2,'test3',1);

-- COMMAND ----------

select * from product_clone;

-- COMMAND ----------

create table product_shallow shallow clone product_new;
