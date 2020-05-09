import re
import os
import requests
import logging
import pandas as pd
import numpy as np
import bisect
from bs4 import BeautifulSoup
from io import StringIO
from fundscrape.template.base import normal_size
from tqdm import tqdm_notebook

head_pattern = {'ticker': r"Ticker\s*(?:Symbol)?:?\s*([a-zA-Z0-9]*)",
                'securityID': r"Security:?\s*([a-zA-Z0-9]*)",
                'meetingDate': r"Meeting\s*Date:?\s*(\d{1,2}-\w{3}-\d{2,4})",
                'meetingType': r"Meeting\s*Type:?\s*([a-zA-Z0-9]*)",
                'recordDate': r"Record\s*Date:?\s*(\d{1,2}-\w{3}-\d{2,4})"}

def header(tr):
    if "bgcolor" in tr.attrs:
        return True
    elif ('style' in tr.attrs) and ("background-color" in tr['style'].lower()):
        return True
    else:
        for td in tr.find_all("td"):
            if ("style" in td.attrs) and ("background-color" in td['style'].lower()):
                return True
    return False

def border(tr):
    for td in tr.find_all("td"):
        if ("style" in td.attrs) and ("border-bottom" in td['style'].lower()):
            return True
    return False

def drop_empty(xx):
    return [ii for ii in xx if ii.get_text().replace("\xa0",'').replace("\n",'').replace("For/Against", "").replace(' ','')]

def process(x):
    return x.replace("\xa0",' ').replace("\n",' ').replace("&nbsp;", " ").strip()

def colspan(td):
    try:
        col_span = int(td['colspan'])
    except:
        col_span = 1
    return col_span

def is_header(row):
    return process(row.get_text()).replace(" ","").startswith("ItemProposal")

def parse_html_rows(rows, tqdm_flag=False):
    # n_columns = 0
    n_rows=0
    column_names = []
    col_pos = 0
    col_poses = []
    for th in rows[0].find_all(['td','th']):
        column_names.append(th.get_text().strip())
        col_poses.append(col_pos)
        col_pos += colspan(th)

    column_names = [process(x) for x in column_names]
    # n_columns = len(column_names)
    n_rows = len(rows) - 1

    df = pd.DataFrame(columns = column_names,
                      index = range(0,n_rows),
                      data = "")
    row_marker = 0
#     column_marker = 0
    if tqdm_flag:
        for row in tqdm_notebook(rows[1:]):
            pos = 0
            tds = row.find_all('td')
            for td in tds:
    #             print(pos)
                try:
                    column_marker = bisect.bisect(col_poses, pos)-1
                    df.iloc[row_marker,column_marker] += process(td.get_text())
                except:
                    pass
                pos += colspan(td)
            row_marker += 1
    else:
        for row in rows[1:]:
            pos = 0
            tds = row.find_all('td')
            for td in tds:
    #             print(pos)
                try:
                    column_marker = bisect.bisect(col_poses, pos)-1
                    df.iloc[row_marker,column_marker] += process(td.get_text())
                except:
                    pass
                pos += colspan(td)
            row_marker += 1

    # Convert to float if possible
    for col in df:
        try:
            df[col] = df[col].astype(float)
        except ValueError:
            pass

    df.columns = [re.sub('\s+', " ", column) for column in df.columns]
    df.replace('', np.nan, inplace=True)

    return df

def parse_html_table(table):
    return parse_html_rows(table.find_all('tr'))


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
        bs = BeautifulSoup(text, 'lxml')
        tables = bs.find_all('table')

        rows = []
        for table in tables:
            new_rows = drop_empty(table.find_all("tr"))
            for new_row in new_rows:
                if header(new_row):
                    rows += new_rows
                    break

        ids = [jj for jj in range(len(rows)) if header(rows[jj])]
        # # deal with bad formats
        ids = ids[0:2] + [ids[kk] for kk in range(2, len(ids)) if (ids[kk] > ids[kk-1] + 1) or (ids[kk] == ids[kk-2] + 2)]

        logging.info(f"There are {len(ids)//2} tables in this file.")
        if (len(ids)//2 == 0) and normal_size(text_root + file_name):
            logging.warning(f"Something wrong with {file_name}")

        i=0
        while i < len(ids)-1:
        # for i in range(0,len(ids),2):
            if ("Account" in rows[ids[i]].get_text()) and ("Custodian" in rows[ids[i]].get_text()):
                i += 1
                continue
            else:
                company = process(rows[ids[i]].get_text())
                info = rows[ids[i]+1:ids[i+1]]
                while (i < len(ids)-1) and is_header(rows[ids[i+1]]):
                    if i+2 < len(ids):
                        npx_table = rows[ids[i+1]:ids[i+2]]
                    else:
                        npx_table = rows[ids[i+1]:]
                    head_info = {}
                    for row in info:
                        row = process(row.get_text())
                        for key, value in head_pattern.items():
                            try:
                                head_info[key] = re.sub(r'\s{1,}', ' ', re.search(value, row).group(1).strip())
                            except:
                                pass
                    df = parse_html_rows(npx_table)
                    df.fillna(method='ffill',inplace=True)

                    for key, value in head_info.items():
                        df[key] = value
            #         df['fund'] = fund
                    df['company'] = company
                    df['fund_company'] = link_table.loc[n, 'fund_company']
                    df.drop(['Item'], axis=1, inplace=True)
                    if "" in df.columns:
                        df.drop([''], axis=1, inplace=True)
                    if "Management" in df.columns:
                        df.rename(columns={"Management":"Mgt. Rec"}, inplace=True)
                    if "For/Against Management" in df.columns:
                        df.rename(columns={"For/Against Management":"Mgt. Rec"}, inplace=True)

                    if 'ticker' in df.columns:
                        df['ticker'] = df['ticker'].str.replace("Meeting", 'nan')
                    dfs.append(df)
                    i += 1
                i += 1


    results = pd.concat(dfs, sort=False, ignore_index=True)
    logging.info(f"Results shape {results.shape}")
    return results