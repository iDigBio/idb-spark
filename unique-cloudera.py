from __future__ import print_function
import os
import sys
import unicodecsv
from pyspark import SparkContext
from operator import add


import csv
import StringIO

def parse(str, headers=None):
    if headers is not None:
        retval = {}
    else:
        retval = []

    try:

        b = StringIO.StringIO(str)
        r = csv.reader(b)

        for line in r:
            i = 0
            for value in line:
                if headers:
                    retval[ headers[i] ] = value
                else:
                    retval.append(value)
                i += 1
    except Exception as e:
        with open("/tmp/rejects", "a") as f:
            f.write("PROBLEM WITH: {0}\n".format(str))

        # assume we're parsing a line and not the headers
        for h in headers:
            retval[h] = ""
        #raise e

    return retval



if __name__ == "__main__":

    #recordset = "00d9fcc1-c8e2-4ef6-be64-9994ca6a32c3"
    #recordset = "0b17c21a-f7e2-4967-bdf8-60cf9b06c721"
    recordset = "idigbio"

#    fn = "data/idigbio_all_2015_08_25/occurrence.csv"
#    fn = "hdfs://cloudera0.acis.ufl.edu:8020/user/admin/idb_all_20150622/occurrence.csv"
    fn = "hdfs://cloudera0.acis.ufl.edu:8020/user/mcollins/occurrence.csv"


    out_dir = "out_{0}".format(recordset)
    if not os.path.exists(out_dir):
        os.makedirs(out_dir)

    sc = SparkContext(appName="UniqueCSVline")

    records = sc.textFile(fn)
    first_line = records.take(1)[0]
    headers = parse(first_line)

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
    #fields = ["dwc:genus"]
    fields = headers


    # filter removes header line which is going to be unique
    records = records.filter(lambda line: line != first_line)
    parsed = records.map(lambda x: parse(x.encode("utf8"), headers) )
    parsed.cache()

    for field in fields:

        out_fn = "{0}/unique_{1}.csv".format(out_dir, field.replace(":", "_"))
        if os.path.exists(out_fn):
            continue

        counts = parsed.map(lambda x: (x.get(field, ""), 1))
        totals = counts.reduceByKey(add)

#        totals.saveAsTextFile("hdfs://cloudera0.acis.ufl.edu:8020/user/mcollins/idigbio_out/out_{0}".format(field.replace(":", "_")))

        # http://apache-spark-user-list.1001560.n3.nabble.com/Iterator-over-RDD-in-PySpark-td11146.html
        iter = totals._jrdd.toLocalIterator()
        output = totals._collect_iterator_through_file(iter)
#        output = totals.collect()
        with open(out_fn, "wb") as f:
            csvwriter = unicodecsv.writer(f, "excel")
            for (word, count) in output:
                csvwriter.writerow([word, count])

        
