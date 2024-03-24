# Databricks notebook source
from datetime import datetime
from pyspark.sql.functions import current_date,lit
year = datetime.now().strftime('%Y')
month = datetime.now().strftime('%m')
date  = datetime.now().strftime('%d')

Tables = ['PRODUCTS', 'S_ORDER', 'S_ORDER_ITEM']

for table in Tables:
    input_path = '/mnt/bronze/ODIADMIN/'+table+'/'+year+'/'+month+'/'+date+'/'+table+".csv"
    output_path='/mnt/silver/ODIADMIN/'+table+'/'+year+'/'+month+'/'+date+'/'+table
    df = spark.read.csv(input_path,header=True)
    df = df.withColumn('DataExportDate',current_date()).withColumn('DataSource',lit('ADF'))
    df.write.format('delta').mode('overwrite').save(output_path)


# COMMAND ----------


