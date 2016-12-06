import sys
from pyspark import SparkContext, SQLContext
import pyspark.sql.functions as sql
import pyspark.sql.types as types

import nltk
from nltk.corpus import stopwords


sc = SparkContext(appName="BHLTF")
sqlContext = SQLContext(sc)

bhl_df_version = "20160516"
bhl_df = sqlContext.read.load("/guoda/data/bhl-{0}.parquet"
                              .format(bhl_df_version))


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


(bhl_df
    .withColumn("tokens", udf_tokenize(sql.column("ocrtext")))
    .select(sql.column("itemid"),
            sql.explode(sql.column("tokens")).alias("token")
           )
    .groupBy(sql.column("itemid"), sql.column("token"))
    .count()
    .write
    .mode("overwrite")
    .parquet("/guoda/data/bhl-{}-tf.parquet".format(bhl_df_version))
)


# 4:09 on mesos1 with 96 cores
#time HADOOP_USER_NAME=hdfs spark-submit --master mesos://mesos01.acis.ufl.edu:5050 --executor-memory 20G --driver-memory 10G --total-executor-cores 96 bhl_tf_index.py

# still hangs
sys.exit()
