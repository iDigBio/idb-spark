import sys
import re

from pyspark import SparkContext, SQLContext
import pyspark.sql.functions as sql
import pyspark.sql.types as types


sc = SparkContext(appName="iDigBioUniqueCSV")
sqlContext = SQLContext(sc)

idb_df_version = "20161119"  # Hardcoded version of the idb parquet to use
df = sqlContext.read.load("/guoda/data/idigbio-{0}.parquet".format(idb_df_version))

#df = adf.select(adf["data.dwc:specificepithet"], adf["data.dwc:genus"])

field_set = set()
for s in df.schema:
    if not str(s.dataType).startswith("StructType"):
        field_set.add(s.name)
#    else:
#        for sub in s.dataType:
#            field_set.add(".".join([s.name, sub.name]))

#field_set = ["dwc:specificepithet", "dwc:genus"]
#field_set = ["stateprovince", "specificepithet", "data.dwc:specificepithet"]
#field_set = ["data.dwc:specificepithet"]

field_set = list(field_set)[0:10]

p = re.compile('[\W_]+')
for field in field_set:
    slug = p.sub("_", field)
    output_fn = "idigbio-{0}-unique-{1}".format(idb_df_version, slug)
    (df
     .groupBy(df[field])
     .count()
     .write
     .format("com.databricks.spark.csv")
     .mode("overwrite")
     .option("header", "false")
     .save("/outputs/{0}.csv".format(output_fn))
    )

sys.exit(0)
