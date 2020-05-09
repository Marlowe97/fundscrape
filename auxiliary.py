"""
File:           auxiliary.py
Version:        0.1
Author:         Marlowe Zhong
Creation Date:  Saturday, May 2nd 2020, 10:33:30 pm
-----
Last Modified:  Saturday, May 9th 2020, 9:54:30 am
Modified By:    Marlowe Zhong (marlowezhong@gmail.com)
"""

import os
from bs4 import BeautifulSoup

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