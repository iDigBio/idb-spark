from __future__ import print_function
import os
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

    recordset = "00d9fcc1-c8e2-4ef6-be64-9994ca6a32c3"
    #recordset = "0b17c21a-f7e2-4967-bdf8-60cf9b06c721"
    #recordset = "idigbio"

    fn = "data/{0}/occurrence.csv".format(recordset)
    headers = get_headers(fn)

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
    #fields = ["dwc:waterBody"]
    fields = headers


    out_dir = "out_{0}".format(recordset)
    if not os.path.exists(out_dir):
        os.makedirs(out_dir)

    sc = SparkContext(appName="UniqueCSVline")
    csvline = Csvline()

    # filter removes header line which is going to be unique
    records = sc.textFile(fn)
    first_line = records.take(1)[0]
    records = records.filter(lambda line: line != first_line)
    parsed = records.map(lambda x: csvline.parse(x.encode("utf8"), headers) )
    parsed.cache()

    for field in fields:
        #counts = records.map(lambda x: (csvline.parse(x.encode("utf8"), headers)[field], 1))
        counts = parsed.map(lambda x: (x[field], 1))
        ####sorted = counts.sortByKey()
        totals = counts.reduceByKey(add)
        #####totals.saveAsTextFile("out_{0}".format(field.replace(":", "_")))

        output = totals.collect()
        out_fn = "{0}/unique_{1}.csv".format(out_dir, field.replace(":", "_"))
        with open(out_fn, "wb") as f:
            csvwriter = unicodecsv.writer(f, "excel")
            for (word, count) in output:
                csvwriter.writerow([word, count])
