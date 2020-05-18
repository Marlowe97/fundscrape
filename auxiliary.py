"""
File:           auxiliary.py
Version:        0.1
Author:         Marlowe Zhong
Creation Date:  Saturday, May 2nd 2020, 10:33:30 pm
-----
Last Modified:  Sunday, May 17th 2020, 9:13:02 pm
Modified By:    Marlowe Zhong (marlowezhong@gmail.com)
"""

import os
import numpy as np
from fundscrape.database import NpxVote
from bs4 import BeautifulSoup
from functools import reduce

def normal_size(path, threshold = 30):
    fsize = os.path.getsize(path)
    fsize = fsize/float(1024)
    return fsize > threshold

def process_bs(x):
    return x.get_text(strip=True).replace("\xa0", " ").strip()

def at_least_n_field(tr, threshold=5):
    tds = [td for td in tr.find_all('td') if process_bs(td)]
    return bool(len(tds) > threshold)

def check_cols(dfs):
    for df in dfs:
        if len(df.columns) > len(set(df.columns)):
            yield df

def insert_dataframe_into_database(df, columns, missing_columns=None,
database="../../npx_vote.db", table='vote_record'):
    dataIO = NpxVote(database)
    if missing_columns:
        for column in missing_columns:
            df[column] = np.nan
    df['parent_fund_company'] = os.path.basename(os.getcwd())
    dataIO.insert_dataframe(df[columns], table=table)
    dataIO.close()

def operator_or(a, b):
    return a or b

def map_columns(column_map, df):
    df.fillna("", inplace=True)

    for column_name, map_column_list in column_map.items():
        df[column_name] = df[map_column_list].apply(lambda x: reduce(operator_or, x), axis=1)

    df = df.replace("", np.nan)

    return df