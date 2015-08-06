from __future__ import print_function
import sys
import unicodecsv
from pyspark import SparkContext
from operator import add
from lib.csvline import Csvline

def get_headers(fn):
    csvline = Csvline()
    with open(fn, "r") as f:
        return csvline.parse(f.readline())


if __name__ == "__main__":

    fields = ["dwc:scientificName",
              "dwc:specificEpithet",
              "dwc:collectionCode",
              "dwc:occurrenceID",
              "dwc:institutionCode",
              "dwc:genus",
              "dwc:verbatimLocality",
              "dwc:catalogNumber",
              "dwc:eventDate",
              "dwc:recordedBy"]


    #fields = ["dwc:occurrenceID"]

    #fn = "data/idigbio/occurrence.csv"
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
        #totals.saveAsTextFile("out_{0}".format(field.replace(":", "_")))

        output = totals.collect()
        out_fn = "out/unique_{0}.csv".format(field.replace(":", "_"))
        with open(out_fn, "wb") as f:
            csvwriter = unicodecsv.writer(f, "excel")
            for (word, count) in output:
                csvwriter.writerow([word, count])
