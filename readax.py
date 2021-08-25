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

    # total robustness for SK
    __robustness = 0
    # iterate number of es(i)'s neighbour, include es(i) itself
    __iter_num = 0

    __sk_server = []

    ESI_PESUDO = 99999
    DEBUG = True

    def __init__(self, neighbour_csv, K=10):
        self.__K = K
        self.__df = pd.DataFrame(pd.read_csv(neighbour_csv, sep=",", dtype={"site" : "Int64", "nei_site" : "Int64", "distance" : "Int64", "counts" : "Int64"}))
        self.__es = self.__df["site"].unique()

    def __del__(self):
        self.__sk_candidate.clear()
        self.__sk.clear()
        self.__df.clear()
        self.__es.clear()

    def __get_extra_es(self) -> bool:

        # step 1: find neightbours of sk_candidate
        for i in range(1, len(self.__sk_server)):


        # step 2: random find some edge servers for satisfying K
        remain_df = self.__es[~self.__es["site"].isin(self.__sk_server)]




    def read_a(self):
        for (index_i, esi) in self.__es.iterrows():
            self.__sk_candidate.clear()
            self.__sk_server.clear()
            self.__robustness = 0
            self.__iter_num = 1
            self.__sk_candidate[esi] = self.__df.query('site == @esi and counts == @ESI_PESUDO')["distance"]
            self.__sk_server.append(esi)
            esj_df = self.__df.query('site == @esi and counts != @ESI_PESUDO and counts > 0').sort_values(by='counts', axis=0, ascending=False)
            if (DEBUG == True):
                print(esj_df)
            for (index_j, esj) in esj_df.iterrows():
                if (self.__iter_num < self.__K):
                    self.__sk_candidate[esj] = esj["counts"]
                    self.__sk_server.append(esj)
                    self.__iter_num = self.__iter_num + 1
                else:
                    if (DEBUG == True):
                        print("candidate of solution:{}".format(self.__sk_candidate))
                    break
            if (self.__iter_num < self.__K):
                if (__get_extra_es() == False):
                    print("there are not enough edge servers this time")
                    break


    def __print_info(self):
        sum1 = pd.DataFrame(self.__df[self.__df["counts"] != ESI_PESUDO])["counts"].sum()
        sum2 = pd.DataFrame(self.__df[self.__df["counts"] == ESI_PESUDO])["distance"].sum()
        print("solution includes users:{}\ntotoal users:{}\nrobust value:{}".format(self.__sk["sum"], sum1 + sum2, round(self.__sk["sum"] / (sum1 + sum2), 4)))

if __name__ == "__main__":
    ins = ReadA("./data/neighbour-full.csv", 10)
    ins.read_a()
