from __future__ import print_function
import sys
from pyspark import SparkContext
from operator import add


if __name__ == "__main__":

    sc = SparkContext(appName="TestUnique")
    lines = sc.textFile("data/00d9fcc1-c8e2-4ef6-be64-9994ca6a32c3/occurrence.csv", 1)
    fields = lines.map(lambda x: x.split(",")[10])
    counts = fields.map(lambda x: (x, 1))
    totals = counts.reduceByKey(add)

    #output = totals.collect()
    #for (word, count) in output:
    #    print("{0} {1}".format(word, count))

    totals.saveAsTextFile("out")
