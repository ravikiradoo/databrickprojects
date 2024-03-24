# Databricks notebook source
from datetime import datetime
from pyspark.sql.functions import current_date,lit,col,date_format,to_date
year = datetime.now().strftime('%Y')
month = datetime.now().strftime('%m')
date  = datetime.now().strftime('%d')

Tables = ['PRODUCTS', 'S_ORDER', 'S_ORDER_ITEM']

product_input_path = '/mnt/bronze/ODIADMIN/PRODUCTS'+'/'+year+'/'+month+'/'+date+'/'+'PRODUCTS.csv'
s_order_input_path = '/mnt/bronze/ODIADMIN/S_ORDER'+'/'+year+'/'+month+'/'+date+'/'+'S_ORDER.csv'
s_order_item_input_path = '/mnt/bronze/ODIADMIN/S_ORDER_ITEM'+'/'+year+'/'+month+'/'+date+'/'+'S_ORDER_ITEM.csv'

product_output_path = '/mnt/silver/ODIADMIN/PRODUCTS'+'/'+year+'/'+month+'/'+date+'/'+'PRODUCTS'
s_order_output_path = '/mnt/silver/ODIADMIN/S_ORDER'+'/'+year+'/'+month+'/'+date+'/'+'S_ORDER'
s_order_item_output_path = '/mnt/silver/ODIADMIN/S_ORDER_ITEM'+'/'+year+'/'+month+'/'+date+'/'+'S_ORDER_ITEM'


products = spark.read.format('csv').load(product_input_path,header=True)
s_order = spark.read.format('csv').load(s_order_input_path,header=True)
s_order_item = spark.read.format('csv').load(s_order_item_input_path,header=True)

products=products.select(col('PRODUCTID').alias('PROD_ID'),col('PRODUCTNAME').alias('PROD_NAME'),col('CATEGORY').alias('PROD_CAT'))
s_order=s_order.select('ORDER_ID','ORDER_STATUS','STORE_ID','ORDER_DATETIME').withColumn('ORDER_DATE',to_date(col("ORDER_DATETIME"))).drop('ORDER_DATETIME')
s_order_item.select('ORDER_ID','LINE_ITEM_ID','PRODUCT_ID','UNIT_PRICE','QUANTITY')

products.write.format('delta').option("mergeSchema", "true").mode('overwrite').save(product_output_path)
s_order.write.format('delta').option("mergeSchema", "true").mode('overwrite').save(s_order_output_path)
s_order_item.write.format('delta').option("mergeSchema", "true").mode('overwrite').save(s_order_item_output_path)



