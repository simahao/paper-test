import numpy as np
import pandas as pd


def read_a(K=400):
    df = pd.DataFrame(pd.read_csv("./data/neighbour150-200 copy.csv", sep=","))
    site = 0
    sk = {}
    neighbours = {}
    for (index, row) in df.iterrows():
        benefit_sum = 0 
        if (pd.isna(row["nei_site"])):
            benefit_esi = row["counts"]
            continue
        if (row["site"] != site and site != 0):
            # if there isn't more es(i)'s neighbour
            for (key, value) in neighbours.items():
                if (benefit_esi + benefit_sum < K):
                    sk[key] = value
                    benefit_sum = benefit_sum + value
            
            benefit_esi = 0
            neighbours.clear()
        else:
            neighbours[row["nei_site"]] = row["counts"]
        site = row["site"]


if __name__ == "__main__":
    read_a()
