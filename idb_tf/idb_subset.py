from pyspark import SparkContext, SQLContext
import pyspark.sql.functions as sql
import pyspark.sql.types as types


sc = SparkContext(appName="SmallIDB")
sqlContext = SQLContext(sc)

idb_df_version = "20161119"
idb_df = sqlContext.read.load("/guoda/data/idigbio-{0}.parquet"
                              .format(idb_df_version))

#count = idb_df.count()

(idb_df
    .limit(100000)
    .write
    .mode("overwrite")
    .parquet("/guoda/data/idigbio-{}-100k.parquet".format(idb_df_version))
)


# 4:45 on mesos1 with 32 cores
# time HADOOP_USER_NAME=hdfs spark-submit --master mesos://mesos01.acis.ufl.edu:5050 --executor-memory 20G --driver-memory 10G --total-executor-cores 32 idb_subset.py
