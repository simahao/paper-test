import copy

import numpy as np
import pandas as pd


class ReadA:
    """doc"""
    # cadidate of sk
    __sk_candidate = {}

    # target number of edge servers
    __K = 0

    # total number of candidate
    __total = 0

    # iterate number of es(i)'s neighbour, include es(i) itself
    __iter_num = 0

    # save edge server's name
    __sk_server = []

    # save edge server's robust
    __sk_robust = []

    __col = []

    __coverage = []

    __sk_id = 0

    # constants for edge server itself
    ESI_PESUDO = 99999

    DEBUG = True

    def __init__(self, neighbour_csv, K=10):
        self.__K = K
        self.__df = pd.DataFrame(pd.read_csv(neighbour_csv, sep=",", dtype={"site" : "Int64", "nei_site" : "Int64", "distance" : "Int64", "counts" : "Int64"}))
        self.__es = self.__df["site"].unique()
        # because sk_id, len(col)=K+1
        self.__col = [i for i in range(self.__K)]
        self.__col.insert(0, "sk_id")
        self.__rlt_robust = pd.DataFrame(columns=self.__col)
        self.__rlt_server = pd.DataFrame(columns=self.__col)


    def __get_extra_es(self) -> bool:

        # step 1: find neightbours of sk_candidate
        # __sk_server[0] is initial edge server, its counts equals to 99999, exclude it
        tmp_sk_server = copy.deepcopy(self.__sk_server)
        i = 0
        while (True):
            i = i + 1
            if (i == len(tmp_sk_server)):
                break
            nei_nei = self.__df.query('site == @tmp_sk_server[@i] and counts != @self.ESI_PESUDO and counts > 0').sort_values(by="counts", axis=0, ascending=False)
            if (nei_nei.empty):
                continue
            for (index, esj) in nei_nei.iterrows():
                if (self.__iter_num < self.__K):
                    if (esj["nei_site"] in self.__sk_candidate):
                        continue
                    self.__sk_candidate[esj["nei_site"]] = esj["counts"]
                    self.__sk_server.append(esj["nei_site"])
                    self.__sk_robust.append(esj["counts"])
                    self.__iter_num = self.__iter_num + 1
                else:
                    tmp = pd.DataFrame([self.__sk_robust], columns=self.__col)
                    self.__rlt_robust = pd.concat([self.__rlt_robust, tmp], ignore_index=True)
                    tmp = pd.DataFrame([self.__sk_server], columns=self.__col)
                    self.__rlt_server = pd.concat([self.__rlt_server, tmp], ignore_index=True)
                    # self.__sk_robust.clear()
                    if (self.DEBUG == True):
                        if (len(self.__sk_candidate) != self.__K):
                            print("sk_candidate: ", self.__sk_candidate)
                        # print("candidate of solution:{}".format(self.__sk_candidate))
                    break
            if (self.__iter_num == self.__K):
                break

        # step 2: random find some edge servers for satisfying K
        if (self.__iter_num < self.__K):
            remain_df = self.__df.query('counts == @self.ESI_PESUDO')
            remain_df = remain_df[~remain_df["site"].isin(self.__sk_server)]
            # remain_df = self.__es[~self.__es.isin(self.__sk_server)]
            remain_nums = self.__K - self.__iter_num + 100
            rand_index = np.random.randint(0, len(remain_df), remain_nums)
            i = 0
            while (True):
                row = remain_df.iloc[rand_index[i]]
                i = i + 1
                if (row["site"] in self.__sk_candidate):
                    continue
                self.__sk_candidate[row["site"]] = row["distance"]
                self.__sk_server.append(row["site"])
                self.__sk_robust.append(row["distance"])
                self.__iter_num = self.__iter_num + 1
                if (self.__iter_num == self.__K):
                    tmp = pd.DataFrame([self.__sk_robust], columns=self.__col)
                    self.__rlt_robust = pd.concat([self.__rlt_robust, tmp], ignore_index=True)
                    tmp = pd.DataFrame([self.__sk_server], columns=self.__col)
                    self.__rlt_server = pd.concat([self.__rlt_server, tmp], ignore_index=True)
                    # self.__sk_robust.clear()
                    if (self.DEBUG == True):
                        if (len(self.__sk_candidate) != self.__K):
                            print("sk_candidate: ", self.__sk_candidate)
                        # print("candidate of solution:{}".format(self.__sk_candidate))
                    break
        if (self.__iter_num < self.__K):
            return False
        return True




    def read_a(self):
        for esi in self.__es:
            self.__sk_candidate.clear()
            self.__sk_server.clear()
            self.__sk_robust.clear()
            self.__robustness = 0
            self.__iter_num = 1

            esi_rubust_df = self.__df.query('site == @esi and counts == @self.ESI_PESUDO')
            if (not esi_rubust_df.empty):
                self.__sk_candidate[esi] = esi_rubust_df.iloc[0, 2]
            else:
                continue
            self.__sk_id = self.__sk_id + 1
            self.__sk_robust.append(self.__sk_id)
            self.__sk_robust.append(esi_rubust_df.iloc[0, 2])
            self.__sk_server.append(self.__sk_id)
            self.__sk_server.append(esi)

            esj_df = self.__df.query('site == @esi and counts != @self.ESI_PESUDO and counts > 0').sort_values(by='counts', axis=0, ascending=False)
            if (esj_df.empty):
                continue
            for (index_j, esj) in esj_df.iterrows():
                if (self.__iter_num < self.__K):
                    self.__sk_candidate[esj["nei_site"]] = esj["counts"]
                    self.__sk_server.append(esj["nei_site"])
                    self.__sk_robust.append(esj["counts"])
                    self.__iter_num = self.__iter_num + 1
                else:
                    tmp = pd.DataFrame([self.__sk_robust], columns=self.__col)
                    self.__rlt_robust = pd.concat([self.__rlt_robust, tmp], ignore_index=True)
                    tmp = pd.DataFrame([self.__sk_server], columns=self.__col)
                    self.__rlt_server = pd.concat([self.__rlt_server, tmp], ignore_index=True)
                    # self.__sk_robust.clear()
                    if (self.DEBUG == True):
                        if (len(self.__sk_candidate) != self.__K):
                            print("sk_candidate: ", self.__sk_candidate)
                        # print("candidate of solution:{}".format(self.__sk_candidate))
                    break
            if (self.__iter_num < self.__K):
                if (self.__get_extra_es() == False):
                    print("there are not enough edge servers this time")
                    break
        self.__rlt_robust["sum"] = self.__rlt_robust.loc[:, 1:].sum(axis=1)
        self.__rlt_robust = self.__rlt_robust.sort_values(by="sum", axis=0, ascending=False, ignore_index=True)

    def extra_coverage(self):
        # tmp = copy.deepcopy(self.__rlt_server)
        tmp = self.__rlt_robust.join(self.__rlt_server.set_index('sk_id'), on='sk_id', how='inner', lsuffix="_left")
        tmp = tmp.iloc[:, len(self.__col) + 1:]
        # iterate each row in rlt for extra coverage
        for (index, row) in tmp.iterrows():
            # get one row as list
            servers = row.to_list()
            cov = 0
            # iterate each element in list, exclude last "sum"
            for i in range(len(servers)):
                # if (i == 0):
                #     continue
                ex_df = self.__df.query('site == @servers[@i] and counts != @self.ESI_PESUDO').sort_values(by='counts', axis=0, ascending=False)
                for (ix, ex) in ex_df.iterrows():
                    if (ex["site"] in self.__sk_server):
                        continue
                    cov = cov +  ex["counts"]
            self.__coverage.append(cov)
        self.__rlt_robust["extra_cov"] = pd.DataFrame(self.__coverage, columns=['extra_cov'])
        rank = self.__rlt_robust[['sum', 'extra_cov']].rank(axis=0, method='first', ascending=False)
        print(rank)




    def __get_sum(self) -> int:
        # unique_df = pd.DataFrame(self.__es, columns=['site'])
        # dfp = self.__df.query('counts == @self.ESI_PESUDO').join(unique_df.set_index('site'), on='site', how='inner')
        # sum = dfp["distance"].sum()
        sum = self.__df.query('counts == @self.ESI_PESUDO')["distance"].sum()
        return sum

    def print_info(self):
        sum = self.__get_sum()
        sk_sum = self.__rlt_robust.at[0, "sum"]
        print("sk robust:{}\ntotal users:{}\nratio:{}".format(sk_sum, sum, round(sk_sum / sum, 2)))

if __name__ == "__main__":
    pd.set_option('display.max_rows', None)
    ins = ReadA("./data/neighbour-full.csv", 5)
    ins.read_a()
    ins.extra_coverage()
    ins.print_info()

