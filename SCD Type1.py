# Databricks notebook source
df = spark.read.option('inferSchema','true').csv('/FileStore/tables/csv/products.csv',header=True);
df.write.saveAsTable('products');

# COMMAND ----------

df = spark.read.option('multiline','true').json('/FileStore/tables/json/products.json');
df.display();
df.createOrReplaceTempView('products_new');

# COMMAND ----------

# MAGIC %sql
# MAGIC merge into products using products_new
# MAGIC on products.ProductID = products_new.ProductID
# MAGIC when matched then
# MAGIC update set
# MAGIC ProductID=products_new.ProductID,
# MAGIC ProductName=products_new.ProductName,
# MAGIC Category=products_new.Category,
# MAGIC ListPrice=products_new.ListPrice
# MAGIC when not matched then
# MAGIC Insert
# MAGIC (ProductID,ProductName,Category,ListPrice)
# MAGIC values(products_new.ProductID,products_new.ProductName,products_new.Category,products_new.ListPrice);
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from products;
