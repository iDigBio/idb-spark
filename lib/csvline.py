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

        i = 0
        for line in r:
            for value in line:
                if headers:
                    retval[ headers[i] ] = value
                else:
                    retval.append(value)
                i += 1

        return retval


if __name__ == "__main__":
    csvline = Csvline()

    with open("../data/0072bf11-a354-4998-8730-c0cb4cfc9517/occurrence.csv", "r") as f:
        headings = csvline.parse(f.readline())
        l = csvline.parse(f.readline(), headings)
        print headings
        print l
