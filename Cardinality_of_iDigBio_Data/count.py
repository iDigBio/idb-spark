import sys
from pyspark import SparkContext
from operator import add


if __name__ == "__main__":

    sc = SparkContext(appName="TestCount")
    lines = sc.textFile("data/allCountries.txt")
    counts = lines.map(lambda x: 1)
    total = counts.reduce(add)
    print total
