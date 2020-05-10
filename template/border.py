"""
File:           border.py
Version:        0.1
Author:         Marlowe Zhong
Creation Date:  Friday, May 8th 2020, 6:52:12 pm
-----
Last Modified:  Saturday, May 9th 2020, 7:25:52 pm
Modified By:    Marlowe Zhong (marlowezhong@gmail.com)
"""


# import packages
import re
import os
import requests
import logging
import pandas as pd
import numpy as np
from bs4 import BeautifulSoup
from io import StringIO
from tqdm import tqdm
from fundscrape.auxiliary import normal_size, process_bs
from fundscrape.template.blue import parse_html_rows

head_pattern = {'ticker': r"(?<=Ticker:)\s*\w*",
                'securityID': r"(?<=Security ID:)\s*\w*",
                'meetingDate': r"(?<=Meeting Date:)\s*\w{3} \d{2}, \d{4}",
                'meetingType': r"(?<=Meeting Type:).*",
                'recordDate': r"(?<=Record Date:)\s*\w{3} \d{2}, \d{4}"}


def process(x, separator='\n'):
    y = x.replace("\xa0"," ").split(separator)
    return [i.strip() for i in y if i.strip()]

def bold(tr):
    for td in tr.find_all("td"):
        if ("style" in td.attrs) and ("bold" in td['style'].lower()):
            return True
        if td.find_all('font') and ("bold" in td.find('font')['style'].lower()):
            return True
    return False

def parse(link_table, text_root='text/'):
    dfs = []
    for n in range(len(link_table)):
        file_name = link_table.loc[n, 'file_name']
        try:
            with open(text_root + file_name) as f:
                text = f.read()
        except:
            logging.warning(f'{file_name} can not open!')
            continue
        logging.info(f"Start to parse {file_name}.")

        bs = BeautifulSoup(text,'lxml')

        if bs.find(text = lambda x: re.search(r'Proxy\s*Voting\s*Records?\.?$', x, flags=re.IGNORECASE)):
            tables = bs.find_all(text = lambda x: re.search(r'Proxy\s*Voting\s*Records?\.?$', x, flags=re.IGNORECASE))[-1].find_all_next('table')
        else:
            tables = bs.find_all('table')

        logging.info(f"There are {len(tables)} vote records in this file.")
        if (len(tables) == 0) and normal_size(text_root + file_name):
            logging.warning(f"Something wrong with {file_name}")
        for table in tables:
            rows = [row for row in table.find_all(['tr', 'td'], recursive=False) if process_bs(row)]
            if len(rows) < 4:
                continue
            # fund = table.find_previous('p',align="center").get_text()

            if len(rows[0].find_all('td')) > 3:
                if bold(rows[0]):
                    df = parse_html_rows(rows)
                else:
                    df = parse_html_rows([cache_columns] + rows)
            else:
                separator = '\n\n'
                sline = process(rows[1].get_text(separator=separator), separator=separator)
                fline = process(rows[0].get_text(separator=separator), separator=separator)
                company = fline[0].replace('\n', '')
                head = {}
                for item in fline[1:]:
                    for key, value in head_pattern.items():
                        try:
                            head[key] = re.search(value, item, flags=re.IGNORECASE).group(0).strip()
                        except:
                            pass
                for item in sline:
                    for key, value in head_pattern.items():
                        try:
                            head[key] = re.search(value, item, flags=re.IGNORECASE).group(0).strip()
                        except:
                            pass
                cache_columns = rows[2]
                df = parse_html_rows(rows[2:])

            if "For" in df.columns or "Against" in df.columns or "None" in df.columns:
                continue
            if len(df.columns) < 4:
                # logging.warning("Bad format.")
                continue
            for key, value in head.items():
                df[key] = value
            # df['fund'] = fund
            df['company'] = company
            df['fund_company'] = link_table.loc[n,'fund_company']
            dfs.append(df)

    results = pd.concat(dfs,sort=False, ignore_index=True)
    logging.info(f"Results shape {results.shape}")
    return results