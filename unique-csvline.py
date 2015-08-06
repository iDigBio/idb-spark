from __future__ import print_function
import sys
from pyspark import SparkContext
from operator import add
from lib.csvline import Csvline

def get_headers(fn):
    csvline = Csvline()
    with open(fn, "r") as f:
        return csvline.parse(f.readline())


if __name__ == "__main__":

    fields = ["dwc:scientificName", "dwc:verbatimLocality"]
    #fields = ["dwc:scientificName"]

    fn = "data/0b17c21a-f7e2-4967-bdf8-60cf9b06c721/occurrence.txt"
    headers = get_headers(fn)

    sc = SparkContext(appName="TestUniqueCSVline")
    csvline = Csvline()

    records = sc.textFile(fn)
    #parsed = records.map(lambda x: csvline.parse(x.encode("utf8"), headers) )
    #parsed.cache()

    for field in fields:
        counts = records.map(lambda x: (csvline.parse(x.encode("utf8"), headers)[field], 1))
        #counts = parsed.map(lambda x: (x[field], 1))
        #sorted = counts.sortByKey()
        totals = counts.reduceByKey(add)
        totals.saveAsTextFile("out_{0}".format(field.replace(":", "_")))

#    output = totals.collect()
#    with open("unique-csvline.out", "w") as f:
#        for (word, count) in output:
#            f.write("{0},{1}\n".format(count, word.encode("utf8")))
