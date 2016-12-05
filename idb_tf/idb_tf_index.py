from pyspark import SparkContext, SQLContext
import pyspark.sql.functions as sql
import pyspark.sql.types as types

import nltk
from nltk.corpus import stopwords


sc = SparkContext(appName="IDBTF")
sqlContext = SQLContext(sc)

idb_df_version = "20161119"
idb_df = sqlContext.read.load("/guoda/data/idigbio-{0}-100k.parquet"
                              .format(idb_df_version))


stopwords_set = set(stopwords.words('english'))
t = nltk.tokenize.treebank.TreebankWordTokenizer() 

def tokenize(s):
    '''
    Take a string and return a list of tokens split out from it
    with the nltk library
    '''

    # word_tokenize uses PunktSentenceTokenizer first, then
    # treebank_word_tokenizer on those so can get nested
    # lists.
    #return nltk.tokenize.word_tokenize(s)

    # this is just the treebank tokenizer
    return [word for word in t.tokenize(s) if word not in stopwords_set]

udf_tokenize = sql.udf(tokenize, types.ArrayType(types.StringType()))


(idb_df
    .select(sql.concat_ws(" ", idb_df["data.dwc:occurrenceRemarks"],
                          idb_df["data.dwc:eventRemarks"],
                          idb_df["data.dwc:fieldNotes"]
                         )
                        .alias("note"),
                        idb_df["uuid"]
            )
    .where(sql.column("note") != "")
    .withColumn("tokens", udf_tokenize(sql.column("note")))
    .select(sql.column("uuid"),
            sql.explode(sql.column("tokens")).alias("token")
           )
    .groupBy(sql.column("uuid"), sql.column("token"))
    .count()
    .write
    .mode("overwrite")
    .parquet("/guoda/data/idigbio-{}-tf.parquet".format(idb_df_version))
)


# 4:45 on mesos1 with 32 cores
# time HADOOP_USER_NAME=hdfs spark-submit --master mesos://mesos01.acis.ufl.edu:5050 --executor-memory 20G --driver-memory 10G --total-executor-cores 32 idb_subset.py
