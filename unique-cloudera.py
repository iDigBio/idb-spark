from __future__ import print_function
import os
import sys
import unicodecsv
from pyspark import SparkContext
from operator import add
#from lib.csvline import Csvline


import csv
import StringIO

class Csvline:

    def parse(self, str, headers=None):
        b = StringIO.StringIO(str)
        r = csv.reader(b)

        if headers:
            retval = {}
        else:
            retval = []

        try:
            i = 0
            for line in r:
                for value in line:
                    if headers:
                        retval[ headers[i] ] = value
                    else:
                        retval.append(value)
                    i += 1
        except:
            # assume we're parsing a line 
            for h in headers:
                retval[h] = ""

        return retval


def get_headers(fn):
    csvline = Csvline()
    with open(fn, "r") as f:
        return csvline.parse(f.readline())


if __name__ == "__main__":

    #recordset = "00d9fcc1-c8e2-4ef6-be64-9994ca6a32c3"
    #recordset = "0b17c21a-f7e2-4967-bdf8-60cf9b06c721"
    recordset = "idigbio"

    fn = "data/{0}/occurrence.csv".format(recordset)
    headers = get_headers(fn)

    fn = "hdfs://cloudera0.acis.ufl.edu:8020/user/admin/idb_all_20150622/occurrence.csv"
#    fn = "hdfs://cloudera0.acis.ufl.edu:8020/user/mcollins/idigbio_all_2015_08_23/records.raw.csv"

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
    #fields = headers
    #fields = ["dwc:genus"]

    out_dir = "out_{0}".format(recordset)
    if not os.path.exists(out_dir):
        os.makedirs(out_dir)

    sc = SparkContext(appName="UniqueCSVline")
    csvline = Csvline()

    # filter removes header line which is going to be unique
    records = sc.textFile(fn, 256)
    first_line = records.take(1)[0]
    records = records.filter(lambda line: line != first_line)
    parsed = records.map(lambda x: csvline.parse(x.encode("utf8"), headers) )
    parsed.cache()

    for field in fields:

        out_fn = "{0}/unique_{1}.csv".format(out_dir, field.replace(":", "_"))
        if os.path.exists(out_fn):
            continue

        #counts = records.map(lambda x: (csvline.parse(x.encode("utf8"), headers)[field], 1))
        counts = parsed.map(lambda x: (x[field], 1))
        ####sorted = counts.sortByKey()
        totals = counts.reduceByKey(add)
#        totals.saveAsTextFile("hdfs://cloudera0.acis.ufl.edu:8020/user/mcollins/idigbio_out/out_{0}".format(field.replace(":", "_")))

        output = totals.collect()
        with open(out_fn, "wb") as f:
            csvwriter = unicodecsv.writer(f, "excel")
            for (word, count) in output:
                csvwriter.writerow([word, count])
