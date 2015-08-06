

if __name__ == "__main__":

    with open("data/00d9fcc1-c8e2-4ef6-be64-9994ca6a32c3/occurrence-head.csv", "r") as f:
        for l in f:
            print l.split(",")[2]

