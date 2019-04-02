from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.types import *

spark = SparkSession \
    .builder \
    .appName("alarm correlation") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()
df = spark.read.format('com.databricks.spark.csv').options(header='true',inferschema='true').load('/home/hadoop/Desktop/paper/alert2.csv')
df.show()
sc = spark.sparkContext
result = df.rdd.map(lambda x:(max(x.ip_dst,x.ip_src)+min(x.ip_dst,x.ip_src),x)).groupByKey().collect()
for i in result:
	tmp = i[1]
	for j in tmp:
		print j
	break
