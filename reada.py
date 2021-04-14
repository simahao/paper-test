import copy

import numpy as np
import pandas as pd


def read_a(K=200):
    df = pd.DataFrame(pd.read_csv("./data/neighbour150-200 copy.csv", sep=",", dtype={"site" : "Int64", "nei_site" : "Int64", "distance" : "Int64", "counts" : "Int64"}))
    # cadidate of sk
    sk_candidate = {"sum" : 0}
    # result dict
    sk = {"sum" : 0}
    # es(i)'s neighbour
    neighbours = {}
    site = 0
    # total benefits of es(i)'s neighbour
    benefit_nei_sum = 0
    benefit_esi = 0
    iter_over_flag = False

    for (index, row) in df.iterrows():
        # if this row'site is not equal to site(old site), it means new iteration
        if (row["site"] != site):
            # this row is es(i) itself
            if (row["counts"] == 99999):
                # if counts==99999, app_users counts is saved in "distance" field, because of convenience for sorting
                benefit_esi = row["distance"]
                benefit_nei_sum = benefit_esi
            # this es(i) has not benefit for itself, but this es(i) has neighbour's benefit
            else:
                benefit_esi = 0
                benefit_nei_sum = row["counts"]
        # it should aggregate total benefit for es(i) and its neighbour
        else:
            # if there isn't more es(i)'s neighbour, compute benefit for joining of es(i)'s neighbour
            # for (key, value) in neighbours.items():
            benefit_nei = row["counts"]
            sk_candidate[row["nei_site"]] = benefit_nei
            benefit_nei_sum = benefit_nei_sum + benefit_nei
            # if benefit greater than or equal K, we have found one solution
            if (benefit_esi + benefit_nei_sum >= K):
                # if benefit >= K，sk_candidate is one of solution
                sk_candidate[site] = benefit_esi
                sk_candidate["sum"] = benefit_esi + benefit_nei_sum
                print("new solution:{}".format(sk_candidate))
                # if benefit of this new solution is greater than maximum's benefit
                # this new solution is become new result
                if (sk_candidate["sum"] > sk["sum"]):
                    sk.clear()
                    sk = copy.deepcopy(sk_candidate)
                    iter_over_flag = True
                # clear sk_candidate for next round compute
                sk_candidate.clear()
        # mark current site_id
        site = row["site"]

    print("last solution:{}".format(sk))

if __name__ == "__main__":
    read_a()
