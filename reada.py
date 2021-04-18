import copy

import numpy as np
import pandas as pd


class ReadA:
    """doc"""
    # cadidate of sk
    __sk_candidate = {}
    # last result
    __sk = {"sum" : 0}
    # current site_id
    __site = 0
    # es(i)'s benefit
    __benefit_esi = 0
    # total benefits of es(i)'s neighbour
    __benefit_nei_sum = 0
    # whether benefit of es(i)'s neighbour is over or not
    __iter_over_flag = False
    # target number of edge servers
    __K = 0

    # iterate number of es(i)'s neighbour, include es(i) itself
    __iter_num = 0

    def __init__(self, neighbour_csv, K=10):
        self.__K = K
        self.__df = pd.DataFrame(pd.read_csv(neighbour_csv, sep=",", dtype={"site" : "Int64", "nei_site" : "Int64", "distance" : "Int64", "counts" : "Int64"}))

    def __del__(self):
        self.__sk_candidate.clear()
        self.__sk.clear()

    def __get_extra_es(self) -> bool:
        extra_es = self.__K - self.__iter_num
        if (extra_es <= 0):
            print("logical has error!")
            return False
        over_flag = False
        old_sk_candidate = copy.deepcopy(self.__sk_candidate)
        for site in old_sk_candidate.keys():
            # if site_id equals to esi
            # logic should exclude key
            if (site == self.__site):
                continue
            # find neighbours of __sk_candidate set according to query condition(site) form __df
            ex_df = pd.DataFrame(self.__df[self.__df.site == site])
            if (ex_df.empty):
                continue
            if (ex_df.iloc[0, 3] == 99999):
                ex_df.iloc[0, 3] = ex_df.iloc[0, 2]
                ex_df.iloc[0, 2] = 99999
                ex_df.sort_values(by=["counts"], ascending=False, inplace=True)
                # print(ex_df)

            for (index, row) in ex_df.iterrows():
                # extra edge server should not exist in sk_candidate
                if (row["distance"] == 99999):
                    continue
                extra_es = extra_es - 1
                self.__sk_candidate[row["nei_site"]] = row["counts"]
                self.__benefit_nei_sum = self.__benefit_nei_sum + row["counts"]
                if (extra_es == 0):
                    over_flag = True
                    break
            if (over_flag == True):
                break

        if (extra_es == 0):
            self.__sk_candidate["sum"] = self.__benefit_nei_sum
            if (self.__sk_candidate["sum"] > self.__sk["sum"]):
                self.__sk.clear()
                self.__sk = copy.deepcopy(self.__sk_candidate)
                self.__sk_candidate.clear()
            return True
        # random find extra es
        candidate_list = self.__sk_candidate.keys()
        # del candidate_list[len(candidate_list) - 1]
        remain_df = self.__df[~self.__df["site"].isin(candidate_list)]
        unique_site = remain_df["site"].unique()
        for extra_site in unique_site:
            if (extra_es == 0):
                self.__sk_candidate["sum"] = self.__benefit_nei_sum
                if (self.__sk_candidate["sum"] > self.__sk["sum"]):
                    self.__sk.clear()
                    self.__sk = copy.deepcopy(self.__sk_candidate)
                    self.__sk_candidate.clear()
                    print("candidate of solution:{}".format(self.__sk))
                break
            benefit = remain_df[remain_df.site == extra_site].iat[0, 3]
            if (benefit == 99999):
                benefit = remain_df[remain_df.site == extra_site].iat[0, 2]
            self.__sk_candidate[extra_site] = benefit
            self.__benefit_nei_sum = self.__benefit_nei_sum + benefit
            extra_es = extra_es - 1

        if (extra_es > 0):
            print("there is no enough edge server for READA")
            return False
        return True

    def read_a(self):

        has_error = False
        for (index, row) in self.__df.iterrows():
            # if this row'site is not equal to site(old site), it means new iteration
            if (row["site"] != self.__site):
                # number of es(i)'s neighbour could not satisfy with condition(K)
                if (self.__site != 0 and self.__iter_num < self.__K ):
                    if (self.__get_extra_es() == False):
                        has_error = True
                        break
                self.__iter_over_flag = False
                if (self.__site != 0):
                    self.__sk_candidate.clear()
                # this row is es(i) itself
                if (row["counts"] == 99999):
                    # if counts==99999, app_users counts is saved in "distance" field, because of convenience for sorting
                    self.__benefit_esi = row["distance"]
                    self.__benefit_nei_sum = self.__benefit_esi
                    self.__sk_candidate[row["site"]] = self.__benefit_esi
                # this es(i) has not benefit for itself, but this es(i) has neighbour's benefit
                else:
                    self.__benefit_esi = 0
                    self.__benefit_nei_sum = row["counts"]
                    self.__sk_candidate[row["nei_site"]] = self.__benefit_nei_sum
                self.__iter_num = 1
            # it should aggregate total benefit for es(i) and its neighbour
            else:
                # if candiate of sk has found, it only should iterate continuely
                if (self.__iter_over_flag == True):
                    continue
                # if there isn't more es(i)'s neighbour, compute benefit for joining of es(i)'s neighbour
                self.__benefit_nei = row["counts"]
                self.__sk_candidate[row["nei_site"]] = self.__benefit_nei
                self.__benefit_nei_sum = self.__benefit_nei_sum + self.__benefit_nei

                self.__iter_num = self.__iter_num + 1

            # if benefit greater than or equal K, we have found one solution
            if (self.__iter_num >= self.__K):
                self.__iter_over_flag = True
                # if __iter_num >= K，__sk_candidate is one of solution
                self.__sk_candidate["sum"] = self.__benefit_nei_sum
                print("candidate of solution:{}".format(self.__sk_candidate))
                # if benefit of this new solution is greater than maximum's benefit
                # this new solution is become new result
                if (self.__sk_candidate["sum"] > self.__sk["sum"]):
                    self.__sk.clear()
                    self.__sk = copy.deepcopy(self.__sk_candidate)
                # clear __sk_candidate for next round compute
                self.__sk_candidate.clear()

            # self.__judge_benefit()
            # mark current site_id
            self.__site = row["site"]
        if (has_error == False):
            print("last solution:{}".format(self.__sk))

if __name__ == "__main__":
    ins = ReadA("./data/neighbour-full.csv", 10)
    ins.read_a()
