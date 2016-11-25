from __future__ import print_function
import sys
from pyspark import SparkContext
from operator import add
from lib.dwca import Dwca


if __name__ == "__main__":

    # rs 00d9fcc1-c8e2-4ef6-be64-9994ca6a32c3, 129 MB .zip, 263 MB uncompressed
    # 9.8 s to decompress
    archive = Dwca("data/0072bf11-a354-4998-8730-c0cb4cfc9517.zip")

    sc = SparkContext(appName="TestUnique")

    # 00:03:42, to parallelize 263 MB
    # 2.8 GB RAM used (probably 16 partitions)
    records = sc.parallelize(archive.core)    
    records.persist()
    one_col = records.map(lambda x: x.get("dwc:recordedBy", ""))
    counts = one_col.map(lambda x: (x, 1))
    totals = counts.reduceByKey(add)

    output = totals.collect()
    with open("unique-dwa.out", "w") as f:
        for (word, count) in output:
            f.write("{0},{1}\n".format(count, word.encode("utf8")))
