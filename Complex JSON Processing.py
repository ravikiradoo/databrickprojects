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

# COMMAND ----------

df.createOrReplaceTempView('Pizza');

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC (select *,batters.*,explode(topping) from (select *,explode(batters.batter) as batter from pizza));

# COMMAND ----------

# MAGIC %sql
# MAGIC select id,name,collect_set(batter_id), collect_set(topping_id) from Pizza group by id,name;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from samples.nyctaxi.trips;

# COMMAND ----------

df = spark.read.csv('/databricks-datasets/online_retail/data-001/data.csv',header=True);
df.display();

# COMMAND ----------

df.createOrReplaceTempView('Pizza');

# COMMAND ----------

# MAGIC %sql
# MAGIC select id ,batters.batter,transform(batters.batter,b -> CAST(character_length(b.type) as INT)) as typelength from pizza;

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace function type_code(batter_type string)
# MAGIC returns int
# MAGIC return case when batter_type = "Chocolate" then 1
# MAGIC          when batter_type="Regular" then 2
# MAGIC         else 3
# MAGIC         end;

# COMMAND ----------

# MAGIC %sql
# MAGIC select id,type_code(batter_type) from pizza;
