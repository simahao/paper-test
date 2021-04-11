import pandas as pd


def read_a(K):
    sk = {}
    df = pd.DataFrame(pd.read_csv("./data/neighbour150-200.csv", sep=","))
    site = 0
    for index, row in df.iterrows():
        benefit = int(0)
        nei_list = []
        if (row["site_id"] != site):
            print("aa")
        else:
            for nei in nei_list:
                print(nei)


if __name__ == "__main__":
    k = int(10)
    read_a(k)
