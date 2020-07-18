"""
File:           left_right.py
Version:        0.1
Author:         Marlowe Zhong
Creation Date:  Friday, May 8th 2020, 6:52:19 pm
-----
Last Modified:  Friday, May 29th 2020, 10:40:50 pm
Modified By:    Marlowe Zhong (marlowezhong@gmail.com)
"""

import re
import os
import sys
import pandas as pd
import logging
from fundscrape.auxiliary import normal_size
from bs4 import BeautifulSoup

head_pattern = {'ticker':"Ticker\s*", 'securityID':"Security ID:\s*",
                'meetingDate': "Meeting Date\s*", 'meetingType': "Meeting Type\s*",
                'meetingStatus': "Meeting Status\s*",
                'country':"Country of Trade\s*"}

def parse_html_table(table):
    n_columns = 0
    n_rows=0
    column_names = []

    # Find number of rows and columns
    # we also find the column titles if we can
    for i, row in enumerate(table):

        # Determine the number of rows in the table
        td_tags = row.find_all('td')
        if (len(td_tags) > 0) and (len(td_tags) == n_columns):
            n_rows+=1

        # Handle column names if we find them
        if i==0:
            for th in td_tags:
                column_names.append(th.get_text(strip=True))
                n_columns = len(column_names)

    # Safeguard on Column Titles
#     if len(column_names) > 0 and len(column_names) != n_columns:
#         print(column_names)
#         print(n_columns)
#         raise Exception("Column titles do not match the number of columns")

    columns = column_names if len(column_names) > 0 else range(0,n_columns)
    df = pd.DataFrame(columns = columns,
                          index= range(0,n_rows))
    row_marker = 0
    for row in table[1:]:
        column_marker = 0
        columns = row.find_all('td')
        if len(columns) == n_columns:
            for column in columns:
                df.iat[row_marker,column_marker] = column.get_text(strip=True)
                column_marker += 1
            row_marker += 1

    # Convert to float if possible
    for col in df:
        try:
            df[col] = df[col].astype(float)
        except ValueError:
            pass

    return df

def process(x):
    return x.replace("\xa0"," ")

def bottom(tr):
    for td in tr.find_all('td'):
        if 'border-bottom' in td['style']:
            return True
    return False

def top(tr):
    if tr.find('hr'):
        return True

    num = 0
    for td in tr.find_all('td'):
        try:
            if 'border-top' in td['style'].lower():
                num += 1
        except:
            continue
    if num > 3:
        return True
    else:
        return False

def bold(tr):
    for td in tr.find_all('td'):
        if td.find('b'):
            return True
        elif td.find('p'):
            if 'bold' in td.find('p')['style']:
                return True
    return False


def parse(link_table, text_root='text/'):
    dfs = []
    for n in range(len(link_table)):
        file_name = link_table.loc[n, 'file_name']
        # open the file
        try:
            with open(text_root + file_name) as f:
                text = f.read()
        except:
            logging.warning(f'{file_name} can not open!')
            continue

        logging.info(f"Start to parse {file_name}.")
        bs = BeautifulSoup(text,'lxml')
        tables = bs.find_all('table')
        logging.info(f"There are {len(tables)} tables in this file.")
        if (len(tables) == 0) and normal_size(text_root + file_name):
            logging.warning(f"Something wrong with {file_name}")

        for table in tables:
            if len(table.find_all('tr')) < 5:
                continue
            rows = table.find_all('tr')
            split_pos = []
            for i, tr in enumerate(rows):
                if top(tr):
                    split_pos.append(i)
            if len(split_pos) > 10:
                split_pos = split_pos + [None]
                fund = re.search("[Fund|Portfolio] Name\s*: (.*)",
                                process("\n".join([x.get_text() for x in rows[:split_pos[0]]]))).group(1)
                for j in range(1, len(split_pos)-1):
                    segments = [row for row in rows[split_pos[j]+1:split_pos[j+1]] if process(row.get_text()).strip()]
                    if len(segments) < 5:
                        continue
                    company = segments[0].get_text().replace('\n', " ").strip()
                    k = 1
                    head = {}
                    while True:
                        row = segments[k]
                        num = 0
                        for l, td in enumerate(row.find_all('td')):
                            for key, value in head_pattern.items():
                                if re.search(value, td.get_text().strip()):
                                    head[key] = segments[k+1].find_all('td')[l].get_text(strip=True)
                                    num += 1
                        if num == 0:
                            break
                        else:
                            k += 2
                    content = segments[k:]
                    df = parse_html_table(content)

                    for key, value in head.items():
                        df[key] = value

                    if df is not None and df.shape[1]>4:
                        df['fund_company'] = link_table.loc[n,'fund_company']
                        df['fund_name'] = fund
                        df['company'] = company
                        df['link'] = link_table.loc[n,'file_link']
                        for column in df.columns:
                            if not column.strip():
                                df = df.drop(columns=column,axis=1)
                                break
                        dfs.append(df)

    results = pd.concat(dfs,sort=False, ignore_index=True)
    logging.info(f"Results shape {results.shape}")
    return results