import re

from pyspark import SparkContext, SQLContext
import pyspark.sql.functions as sql
import pyspark.sql.types as types


sc = SparkContext(appName="iDigBioUniqueCSV")
sqlContext = SQLContext(sc)

idb_df_version = "20161119"  # Hardcoded version of the idb parquet to use
df = sqlContext.read.load("/guoda/data/idigbio-{0}.parquet".format(idb_df_version))
small_df = (df
            .where(df["stateprovince"] == "vermont")
            .where(df["genus"] == "acer")
            )
print(small_df.count())
small_df.printSchema()
fields = ["stateprovince", "specificepithet", "data.dwc:specificepithet"]
p = re.compile('[\W_]+')
for field in fields:
    slug = p.sub("_", field)
    output_fn = "idigbio-{0}-unique-{1}".format(idb_df_version, slug)
    (small_df
     .groupBy(df[field])
     .count()
     .write
     .format("com.databricks.spark.csv")
     .mode("overwrite")
     .option("header", "false")
     .save("/outputs/{0}.csv".format(output_fn))
    )

