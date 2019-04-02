# -*- coding: UTF-8 -*-
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.types import *
import scapy
from scapy.all import *
from scapy.utils import PcapReader
import pymysql
def sstflow(data,sensitive):
    data = str(data)
    for i in range(len(sensitive)):
        #print (sensitive[i])
        if data.find(sensitive[i])>=0:
            return True
    return False
db = pymysql.connect('192.168.138.131','root','123456','Windows',use_unicode=True,charset='utf8')
cursor = db.cursor()
cursor.execute("select data from data;")
sensitive = []
tempsensitive = cursor.fetchall()
for i in tempsensitive:
    tmp = i[0].encode('utf-8')
    sensitive.append(tmp)
cursor.execute("select path from sstpath;")
tempsensitive = cursor.fetchall()
for i in tempsensitive:
    #print i[0]
    tmp = i[0].encode('utf-8')
    sensitive.append(tmp)
spark = SparkSession \
    .builder \
    .appName("alarm correlation") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()
df = spark.read.format('com.databricks.spark.csv').options(header='true',inferschema='true',multiLine=True,escape='"').load('/home/hadoop/Desktop/paper/test.csv')
sc = spark.sparkContext
sensitive = sc.broadcast(sensitive)
result = df.rdd.filter(lambda x:sstflow(x.msg,sensitive.value)).collect()
dataframe = spark.createDataFrame(result)
dataframe = dataframe.toPandas()
dataframe.to_csv("flow.csv",index=False,encoding='utf-8')

