# Databricks notebook source
df = spark.read.option('multiline','true').json('/FileStore/tables/json/better.json');
df.display();

# COMMAND ----------

from pyspark.sql.functions import *;
df = df.withColumn('topping',explode('topping')).withColumn("topping_id",col("topping.id")).withColumn("topping_type",col("topping.type")).drop('topping');
df.display();

# COMMAND ----------

df =df.withColumn('batters',explode('batters.batter')).withColumn('batter_id',col('batters.id')).withColumn('batter_type',col('batters.type')).drop('batters');
df.display();
