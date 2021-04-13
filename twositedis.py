import math

import numpy as np
import pandas as pd
from geopy.distance import geodesic


def calculate_dis():
    site_df = pd.DataFrame(pd.read_csv('./data/site.csv', sep = ','))
    nei_df = pd.DataFrame(columns=["site", "nei_site", "distance"])
    sta = pd.DataFrame(pd.read_csv("./data/device-site.csv", sep=','))
    app_users = int(0)

    for index_out, row_out in site_df.iterrows():
        for index_inner, row_inner in site_df.iterrows():
            if (row_out["SITE_ID"] != row_inner["SITE_ID"]):
                distance = round(geodesic((row_out["LATITUDE"], row_out["LONGITUDE"]), (row_inner["LATITUDE"], row_inner["LONGITUDE"])).m)
                if (distance >= 150 and distance <= 200):
                    if (not sta[sta.SITE_ID == row_inner["SITE_ID"]].empty):
                        # find row_out's neighbour which is connected by how many users
                        app_users = sta[sta.SITE_ID == row_inner["SITE_ID"]].iat[0, 1]
                    if (app_users is not None):
                        nei_df = nei_df.append({"site" : row_out["SITE_ID"], "nei_site" : row_inner["SITE_ID"], "distance" : distance, "counts" : app_users}, ignore_index=True)
                        app_users = None
            else:
                # how many users are connected to row_out's site
                if (not sta[sta.SITE_ID == row_out["SITE_ID"]].empty):
                    app_users = sta[sta.SITE_ID == row_out["SITE_ID"]].iat[0, 1]
                    nei_df = nei_df.append({"site" : row_out["SITE_ID"], "nei_site" : np.nan, "distance" : app_users, "counts" : 99999}, ignore_index=True)

    nei_df["site"] = nei_df["site"].astype('Int64')
    nei_df["nei_site"] = nei_df["nei_site"].astype('Int64')
    nei_df["distance"] = nei_df["distance"].astype('Int64')
    nei_df["counts"] = nei_df["counts"].astype('Int64')
    nei_df.sort_values(by=["site", "counts"], ascending=[True, False], inplace=True)

    nei_df.to_csv("./data/neighbour150-200.csv", index=False)

if __name__ == "__main__":
    calculate_dis()
    # gen_device_site()
