# Databricks notebook source
#Read CSV File
df = spark.read.option('inferScehma','True').csv('/FileStore/tables/csv/products.csv',header=True);
df.display();

# COMMAND ----------

df.createOrReplaceTempView('product_view');

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from product_view;

# COMMAND ----------

# MAGIC %sql
# MAGIC show views;

# COMMAND ----------

df.createOrReplaceGlobalTempView('product_global_view');

# COMMAND ----------

# MAGIC %sql
# MAGIC show views in global_temp;
