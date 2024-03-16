# Databricks notebook source
# MAGIC %sql
# MAGIC COPY INTO ORDERITEM FROM 'dbfs:/FileStore/ORDERITEMSTREAM/' FILEFORMAT = CSV FORMAT_OPTIONS ('header'='true');

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from orderitem;
