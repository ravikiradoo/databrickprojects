# Databricks notebook source
from datetime import datetime
from pyspark.sql.functions import from_utc_timestamp,current_date,col,round
year = datetime.now().strftime('%Y')
month = datetime.now().strftime('%m')
date  = datetime.now().strftime('%d')
product_input_path = '/mnt/silver/ODIADMIN/PRODUCTS'+'/'+year+'/'+month+'/'+date+'/'+'PRODUCTS'
s_order_input_path = '/mnt/silver/ODIADMIN/S_ORDER'+'/'+year+'/'+month+'/'+date+'/'+'S_ORDER'
s_order_item_input_path = '/mnt/silver/ODIADMIN/S_ORDER_ITEM'+'/'+year+'/'+month+'/'+date+'/'+'S_ORDER_ITEM'

products = spark.read.format('delta').load(product_input_path,header=True)
s_order = spark.read.format('delta').load(s_order_input_path,header=True)
s_order_item = spark.read.format('delta').load(s_order_item_input_path,header=True)

order_df = s_order.join(s_order_item, 'ORDER_ID').join(products,s_order_item.PRODUCT_ID==products.PROD_ID)
order_df=order_df.select('ORDER_ID','LINE_ITEM_ID','ORDER_STATUS','STORE_ID','QUANTITY','UNIT_PRICE','PROD_NAME','PROD_CAT')
order_df=order_df.withColumn('TOTAL_AMOUNT',round(col('QUANTITY')*col('UNIT_PRICE'),2))
order_df=order_df.withColumn('TAX',round(col('TOTAL_AMOUNT')*0.1,2))
order_df=order_df.withColumn('TOTAL_AMOUNT_WITH_TAX',round(col('TOTAL_AMOUNT')+col('TAX'),2))
order_df.createOrReplaceTempView('order_view')



# COMMAND ----------

# MAGIC %sql
# MAGIC merge into databrickwsnew.odiadmin.sales as sales_old
# MAGIC using order_view as sales_new on sales_old.order_id=sales_new.ORDER_ID and sales_old.line_item_id=sales_new.LINE_ITEM_ID
# MAGIC when matched then update set
# MAGIC sales_old.ORDER_ID=sales_new.ORDER_ID,
# MAGIC sales_old.LINE_ITEM_ID=sales_new.LINE_ITEM_ID,
# MAGIC sales_old.ORDER_STATUS=sales_new.ORDER_STATUS,
# MAGIC sales_old.STORE_ID=sales_new.STORE_ID,
# MAGIC sales_old.QUANTITY=sales_new.QUANTITY,
# MAGIC sales_old.UNIT_PRICE=sales_new.UNIT_PRICE,
# MAGIC sales_old.PROD_NAME=sales_new.PROD_NAME,
# MAGIC sales_old.PROD_CAT=sales_new.PROD_CAT,
# MAGIC sales_old.TOTAL_AMOUNT=sales_new.TOTAL_AMOUNT,
# MAGIC sales_old.TAX=sales_new.TAX,
# MAGIC sales_old.TOTAL_AMOUNT_WITH_TAX=sales_new.TOTAL_AMOUNT_WITH_TAX
# MAGIC when not matched then insert*;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from databrickwsnew.odiadmin.sales;
