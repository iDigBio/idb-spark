from pyspark import SparkContext, SQLContext
import pyspark.sql.functions as sql
import pyspark.sql.types as types


sc = SparkContext(appName="SmallBHL")
sqlContext = SQLContext(sc)

bhl_df_version = "20160516"
bhl_df = sqlContext.read.load("/guoda/data/bhl-{0}.parquet"
                              .format(bhl_df_version))

#count = bhl_df.count()

(bhl_df
    .limit(100)
    .write
    .mode("overwrite")
    .parquet("/guoda/data/bhl-tiny-{}.parquet".format(bhl_df_version))
)
