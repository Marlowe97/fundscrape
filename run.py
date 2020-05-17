"""
File:           run.py
Version:        0.1
Author:         Marlowe Zhong
Creation Date:  Saturday, May 2nd 2020, 12:52:29 pm
-----
Last Modified:  Saturday, May 9th 2020, 5:18:41 pm
Modified By:    Marlowe Zhong (marlowezhong@gmail.com)
"""

ABS_PATH = "/user/zz2654/python/"

import os
import sys
sys.path.append(ABS_PATH + "npx_parse")
os.chdir(ABS_PATH + "npx_parse")

from fundscrape.scrape import download
from fundscrape.template import base, two_lines, up_down, blue, border
import logging
import pandas as pd
import numpy as np
from tqdm import tqdm
from fundscrape.database import NpxVote

pd.set_option('display.max_columns', None)
logging.basicConfig(filename='fundscrape/npx.log', filemode='w', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Read the file
category = pd.read_excel("Edgar list2018_v2.xlsx", sheet_name="category")

dataIO = NpxVote()

# Common category
# Set the logging file

# logging.basicConfig(filename='fundscrape/npx_common.log', filemode='w', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

ff_list = category.loc[category.category=="common", "fund_family"]
for ff in tqdm(ff_list[:]):
    ff = ff.lower()
    text_root = f"results_2018/{ff}/text/"
    if not os.path.isdir(f"results_2018/{ff}/"):
        os.makedirs(text_root)
    link_table = download(ff, output_path=text_root, info_path=ABS_PATH + "npx_parse/Edgar list2018_v2.xlsx")
    results = base.parse(link_table, text_root=text_root)
    results['parent_fund_company'] = ff
    columns = ["securityID", "company", "ticker", "fund_company", "parent_fund_company",
               "meetingDate", "meetingType", "Proposal", "Sponsor", "Vote Cast"]
    logging.info("Started to insert into database.")
    dataIO.insert_dataframe(results[columns])


## Two lines category
# There are two lines above and below the company name
# There is no "#" for the column header

# logging.basicConfig(filename='fundscrape/npx_two_lines.log', filemode='w', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

ff_list = category.loc[category.category=="two", "fund_family"]
for ff in tqdm(ff_list[:]):
    ff = ff.lower()
    text_root = f"results_2018/{ff}/text/"
    if not os.path.isdir(f"results_2018/{ff}/"):
        os.makedirs(text_root)
    link_table = download(ff, output_path=text_root, info_path=ABS_PATH + "npx_parse/Edgar list2018_v2.xlsx")
    results = two_lines.parse(link_table, text_root=text_root)
    results['parent_fund_company'] = ff
    columns = ["securityID", "company", "ticker", "fund_company", "parent_fund_company",
               "meetingDate", "meetingType", "Proposal", "Proposal Type", "Proposal Vote"]
    logging.info("Started to insert into database.")
    dataIO.insert_dataframe(results[columns])

## Up down category
# The header info is not in the same line, instead, they are up and down

# logging.basicConfig(filename='fundscrape/npx_up_down.log', filemode='w', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

ff_list = category.loc[category.category=="double", "fund_family"]
for ff in tqdm(ff_list):
    ff = ff.lower()
    text_root = f"results_2018/{ff}/text/"
    if not os.path.isdir(f"results_2018/{ff}/"):
        os.makedirs(text_root)
    link_table = download(ff, output_path=text_root, info_path=ABS_PATH + "npx_parse/Edgar list2018_v2.xlsx")
    results = up_down.parse(link_table, text_root=text_root)
    results['securityID'] = results['securityID'].str.split(' ').str[1]
    results['parent_fund_company'] = ff
    # logging.info(results.head())
    columns = ["securityID", "company", "ticker", "fund_company", "parent_fund_company",
               "meetingDate", "meetingType", "Description", "Proponent", "Vote Cast"]
    logging.info("Started to insert into database.")
    dataIO.insert_dataframe(results[columns])


## Blue header category
## The header is in a blue div

# logging.basicConfig(filename='fundscrape/npx_blue.log', filemode='w', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

ff_list = category.loc[category.category=="blue head", "fund_family"]
for ff in tqdm(ff_list):
    ff = ff.lower()
    # comment these when I need full log output
    # if os.path.exists(f"results_2018/{ff}/{ff}_npx_v2.csv"):
    #     continue
    text_root = f"results_2018/{ff}/text/"
    if not os.path.isdir(f"results_2018/{ff}/"):
        os.makedirs(text_root)
    link_table = download(ff, output_path=text_root, info_path=ABS_PATH + "npx_parse/Edgar list2018_v2.xlsx")
    if ff == "zacks":
        link_table = link_table[link_table.file_name == "0001398344-18-012405.txt"].reset_index(drop=True)
    results = blue.parse(link_table, text_root=text_root)
    if ('For' in results.columns) and ('Against' in results.columns) and ('Abstain' in results.columns) and ('Vote' not in results.columns):
        results[['For', "Against", "Abstain"]] = results[['For', "Against", "Abstain"]].astype('float', errors='ignore')
        results['Vote'] = results[['For', "Against", "Abstain"]].idxmax(axis=1)
    if 'Type' in results.columns:
        results.rename(columns={'Type': 'Proposed by'}, inplace=True)
    for col in ['ticker', 'Proposed by']:
        if col not in results.columns:
            results[col] = np.nan
    # logging.info(results.head())
    results['parent_fund_company'] = ff
    columns = ["securityID", "company", "ticker", "fund_company", "parent_fund_company",
               "meetingDate", "meetingType", "Proposal", "Proposed by", "Vote"]
    logging.info("Started to insert into database.")
    dataIO.insert_dataframe(results[columns])

## border category

# logging.basicConfig(filename='fundscrape/npx_border.log', filemode='w', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

ff_list = category.loc[category.category=="border", "fund_family"]
for ff in tqdm(ff_list[:]):
    ff = ff.lower()
    text_root = f"results_2018/{ff}/text/"
    if not os.path.isdir(f"results_2018/{ff}/"):
        os.makedirs(text_root)
    link_table = download(ff, output_path=text_root, info_path=ABS_PATH + "npx_parse/Edgar list2018_v2.xlsx")
    if ff == "principal":
        link_table = link_table[link_table.file_name != "0001398344-18-012738.txt"].reset_index(drop=True)
    results = border.parse(link_table, text_root=text_root)
    if 'meetingType' not in results.columns:
        results['meetingType'] = np.nan
    results['parent_fund_company'] = ff
    columns = ["securityID", "company", "ticker", "fund_company", "parent_fund_company",
               "meetingDate", "meetingType", "Proposal", "Proposed By", "Vote Cast"]

    # logging.info(results.head())
    logging.info("Started to insert into database.")
    dataIO.insert_dataframe(results[columns])


## Other formats
# logging.basicConfig(filename='fundscrape/npx.log', filemode='w', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
# for ff in tqdm(category.loc[category.category=="*", "fund_family"]):
#     ff = ff.lower()
#     # comment these when I need full log output
#     # if os.path.exists(f"{ff}/{ff}_npx_v2.csv"):
#     #     continue
#     text_root = f"results_2018/{ff}/text/"
#     if not os.path.isdir(f"{ff}/"):
#         os.makedirs(text_root)
#     scrape.download(ff, output_path=text_root, info_path="E:\\Marlowe\\RA\\npx_parse\\Edgar list2018_v2.xlsx", info_crawl=2)

dataIO.close()