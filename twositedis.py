import math

import numpy
import pandas as pd
from geopy.distance import geodesic


def calculate_dis():
    df = pd.DataFrame(pd.read_csv('./data/site.csv', sep = ','))
    nei = pd.DataFrame(columns=["site", "nei_site", "distance"])
    sta = pd.DataFrame(pd.read_csv("./data/device-site.csv", sep=','))
    app_users = int(0)
    for index_out, row_out in df.iterrows():
        for index_inner, row_inner in df.iterrows():
            if row_out["SITE_ID"] != row_inner["SITE_ID"]:
                distance = round(geodesic((row_out["LATITUDE"], row_out["LONGITUDE"]), (row_inner["LATITUDE"], row_inner["LONGITUDE"])).m)
                if (distance >= 150 and distance <= 200):
                    if (not sta[sta["SITE_ID"] == row_out["SITE_ID"]].empty):
                        app_users = sta[sta["SITE_ID"] == row_out["SITE_ID"]].iat[0, 1]
                    if (app_users is not None):
                        nei = nei.append({"site" : row_out["SITE_ID"], "nei_site" : row_inner["SITE_ID"], "distance" : distance, "counts" : app_users}, ignore_index=True)
                        app_users = None
    nei["counts"] = nei["counts"].astype(numpy.int64)
    nei.to_csv("./data/neighbour150-200.csv", index=False)

if __name__ == "__main__":
    calculate_dis()
    # gen_device_site()
