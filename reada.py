import pandas as pd


def read_a(K=40000):
    sk = {}
    df = pd.DataFrame(pd.read_csv("./data/neighbour150-200 copy.csv", sep=","))
    site = 0
    neighbours = {}
    for index, row in df.iterrows():
        benefit = int(0)
        if (row["site"] != site and site != 0):
            # if there isn't more es(i)'s neighbour
            for (key, value) in neighbours.items():
                if (benefit < K):
                    sk[key] = value
                    benefit = benefit + value
            neighbours.clear()
        else:
            neighbours[row["nei_site"]] = row["counts"]
        site = row["site"]


if __name__ == "__main__":
    read_a()
