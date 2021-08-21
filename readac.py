import copy

import numpy as np
import pandas as pd


class ReadAC:
    """
    C means user coverage, if node's a*R - b*C > 0, this node will be
    added network. ReadA only consider R(rubust) > 0
    """
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

    __alpha = 1
    __beta = 0

    ESI_PESUDO = 99999 
    DEBUG = True 

    def __init__(self, neighbour_csv, K=10, alpha=1, beta=0):
        self.__K = K
        self.__alpha = alpha
        self.__beta = beta
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
            if (ex_df.iloc[0, 3] == self.ESI_PESUDO):
                ex_df.iloc[0, 3] = ex_df.iloc[0, 2]
                ex_df.iloc[0, 2] = self.ESI_PESUDO
                ex_df.sort_values(by=["counts"], ascending=False, inplace=True)
                # print(ex_df)

            for (index, row) in ex_df.iterrows():
                # extra edge server should not exist in sk_candidate
                if (row["distance"] == self.ESI_PESUDO):
                    continue
                if (row["nei_site"] == self.__site):
                    continue
                alpha = row["counts"]
                beta = self.__get_coverage_benefits(row["nei_site"]) 
                if (round(self.__alpha * alpha - self.__beta * beta) > 0):
                    self.__benefit_nei = alpha 
                    self.__sk_candidate[row["nei_site"]] = self.__benefit_nei 
                    self.__benefit_nei_sum = self.__benefit_nei_sum + self.__benefit_nei
                    extra_es = extra_es - 1
                    if (extra_es == 0):
                        over_flag = True
                        break
                else:
                    continue
            if (over_flag == True):
                break

        if (extra_es == 0):
            self.__sk_candidate["sum"] = self.__benefit_nei_sum
            self.__benefit_nei_sum = 0
            if (self.__sk_candidate["sum"] > self.__sk["sum"]):
                self.__sk.clear()
                self.__sk = copy.deepcopy(self.__sk_candidate)
                self.__sk_candidate.clear()
                if (self.DEBUG == True):
                    print("candidate of solution:{}".format(self.__sk))
            return True
        # random find extra es
        candidate_list = self.__sk_candidate.keys()
        # del candidate_list[len(candidate_list) - 1]
        remain_df = self.__df[~self.__df["site"].isin(candidate_list)]
        unique_site = remain_df["site"].unique()
        for extra_site in unique_site:
            if (extra_es == 0):
                self.__sk_candidate["sum"] = self.__benefit_nei_sum
                self.__benefit_nei_sum = 0
                if (self.__sk_candidate["sum"] > self.__sk["sum"]):
                    self.__sk.clear()
                    self.__sk = copy.deepcopy(self.__sk_candidate)
                    self.__sk_candidate.clear()
                    if (self.DEBUG == True):
                        print("candidate of solution:{}".format(self.__sk))
                break
            benefit = remain_df[remain_df.site == extra_site].iat[0, 3]
            if (benefit == self.ESI_PESUDO):
                alpha = remain_df[remain_df.site == extra_site].iat[0, 2]
                beta = self.__get_coverage_benefits(extra_site)  
                if (round(self.__alpha * alpha - self.__beta * beta) > 0):
                    self.__sk_candidate[extra_site] = alpha 
                    self.__benefit_nei_sum = self.__benefit_nei_sum + alpha 
                    extra_es = extra_es - 1

        if (extra_es > 0):
            print("there is no enough edge server for READAC")
            return False
        return True

    def __get_coverage_benefits(self, site_id) -> int:
        """
        A is center node, node C is neighbour of node B, whether node B join node A or not,
        depends on whether node B's alpha*R - beta*C > 0 
        """
        # get sum(users) of es(site)'s neighbour as coverage beneifit for site
        df = self.__df.query('counts != @self.ESI_PESUDO and site == @site_id')
        if (df.empty):
            return 0
        else:
            return df["counts"].max()
            # es(site_id) itself, so return distance(user counts)

    def __print_info(self):
        sum1 = pd.DataFrame(self.__df[self.__df["counts"] != 99999])["counts"].sum()
        sum2 = pd.DataFrame(self.__df[self.__df["counts"] == 99999])["distance"].sum()
        print("solution includes users:{}\ntotoal users:{}\nrobust value:{}".format(self.__sk["sum"], sum1 + sum2, round(self.__sk["sum"] / (sum1 + sum2), 4)))

    def read_ac(self):

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
                # this row is es(i) itself, it's no need to compute benefits with alpha and beta
                if (row["counts"] == self.ESI_PESUDO):
                    # if counts==self.ESI_PESUDO, app_users counts is saved in "distance" field, because of convenience for sorting
                    self.__benefit_esi = row["distance"]
                    self.__benefit_nei_sum = self.__benefit_esi
                    self.__sk_candidate[row["site"]] = self.__benefit_esi
                # this es(i) has not benefit for itself, but this es(i) has neighbour's benefit
                else:
                    self.__benefit_esi = 0
                    alpha = row["counts"] 
                    beta = self.__get_coverage_benefits(row["nei_site"])
                    if (round(self.__alpha * alpha - self.__beta * beta) > 0):
                        self.__sk_candidate[row["nei_site"]] = alpha
                        self.__benefit_nei_sum = alpha 
                    else:
                        continue
                self.__iter_num = 1
            # it should aggregate total benefit for es(i) and its neighbour
            else:
                # if candiate of sk has found, it only should iterate continuely
                if (self.__iter_over_flag == True):
                    continue
                # if there isn't more es(i)'s neighbour, compute benefit for joining of es(i)'s neighbour
                alpha = row["counts"]
                beta = self.__get_coverage_benefits(row["nei_site"]) 
                if (round(self.__alpha * alpha - self.__beta * beta) > 0):
                    self.__benefit_nei = alpha
                    self.__sk_candidate[row["nei_site"]] = self.__benefit_nei
                    self.__benefit_nei_sum = self.__benefit_nei_sum + self.__benefit_nei
                    self.__iter_num = self.__iter_num + 1

            # if benefit greater than or equal K, we have found one solution
            if (self.__iter_num >= self.__K):
                self.__iter_over_flag = True
                # if __iter_num >= K，__sk_candidate is one of solution
                self.__sk_candidate["sum"] = self.__benefit_nei_sum
                self.__benefit_nei_sum = 0
                if (self.DEBUG == True):
                    print("candidate of solution:{}".format(self.__sk_candidate))
                # if benefit of this new solution is greater than maximum's benefit
                # this new solution is become new result
                if (self.__sk_candidate["sum"] > self.__sk["sum"]):
                    self.__sk.clear()
                    self.__sk = copy.deepcopy(self.__sk_candidate)
                # clear __sk_candidate for next rond compute
                self.__sk_candidate.clear()

            # self.__judge_benefit()
            # mark current site_id
            self.__site = row["site"]
        if (has_error == False):
            print("last solution:{}".format(self.__sk))
            self.__print_info()

if __name__ == "__main__":
    ins = ReadAC("./data/neighbour-full.csv", 10, 1, 0)
    ins.read_ac()
    
