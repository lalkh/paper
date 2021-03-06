# -*- coding: UTF-8 -*-
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.types import *
import scapy
from scapy.all import *
from scapy.utils import PcapReader
import pymysql
def ipkey(ip1,ip2):
    if ip1<ip2:
        return ip1+'-'+ip2
    else:
        return ip2+'-'+ip1
spark = SparkSession \
    .builder \
    .appName("alarm correlation") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()
sc = spark.sparkContext
df = spark.read.format('com.databricks.spark.csv').options(header='true',inferschema='true',multiLine=True,escape='"').load('/home/hadoop/Desktop/paper/flow.csv')
df2 =spark.read.format('com.databricks.spark.csv').options(header='true',inferschema='true',multiLine=True,escape='"').load('/home/hadoop/Desktop/paper/alert.csv')
df = df[['timestamp','msg','ip_src','src_port','ip_dst','dst_port']]
result = df.union(df2)
#result.show()
result = result.rdd.map(lambda x:(ipkey(x.ip_src,x.ip_dst),x))
result = result.groupByKey().collect()
for i in result:
    cluster = sc.parallelize(i[1],4)
    clusterdf = spark.createDataFrame(cluster)
    clusterdf = clusterdf.toPandas()
    clusterdf = clusterdf.sort_values(by=['timestamp'])
    clusterdf.to_csv("cluster.csv",index=False,sep=',',encoding='utf-8')



