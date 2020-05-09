"""
File:           two_lines.py
Version:        0.1
Author:         Marlowe Zhong
Creation Date:  Friday, May 8th 2020, 6:29:01 pm
-----
Last Modified:  Saturday, May 9th 2020, 5:16:50 pm
Modified By:    Marlowe Zhong (marlowezhong@gmail.com)
"""


# import packages
import re
import os
import requests
import logging
import pandas as pd
from bs4 import BeautifulSoup
from io import StringIO
from tqdm import tqdm
from fundscrape.template.base import normal_size

head_pattern = {'ticker': r"(?<=Ticker:)\s*\w*",
                'securityID': r"(?<=Security:)\s*\w*",
                'meetingDate': r"(?<=Meeting Date:)\s*\d{2}-\w{3}-\d{4}",
                'meetingType': r"(?<=Meeting Type:).*"}

def drop_empty(xx):
    # do not strip, take care of second line
    return [ii for ii in xx if ii.strip()]

def preprocess(text):
    try:
        if re.search(r'\* Management position unknown', text):
            text = re.search(r'((?:.|\n)*)\* Management position unknown', text).group(1)
    except:
        logging.warning(f"Ending not found in {file_name}!")
        return None
    bs = BeautifulSoup(text, 'lxml')
    text = bs.get_text().replace('\xa0'," ")
    return text

def get_columns(line):
    cnames = []
    cpos = []
    for pos in re.finditer(r"[# ]\s{1,}", line):
        cpos.append(pos.end())
    cpos = [0] + cpos + [None]
    for p in range(len(cpos) - 1):
        cnames.append(line[cpos[p]:cpos[p+1]].strip())
    return cnames, cpos


def parse(link_table, text_root='text/', first_separate = r"[^-\d]\n([^\n:]+)\n{1,3}-{10,}",
          second_separate = r"-{10,}\n([^\n]+)\n-{10,}"):
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

        text = preprocess(text)

        tables = re.split(first_separate, text)
        logging.info(f"There are {(len(tables)-1)//2} sub-funds in this file.")

        if ((len(tables)-1)//2 == 0) and normal_size(text_root + file_name):
            logging.warning(f"Something wrong with {file_name}")
        for i in range(1, len(tables), 2):
            if re.search(second_separate, tables[i+1]):
                segments = [x.strip() for x in re.split(second_separate, tables[i+1]) if x.strip()]
            else:
                continue
            fund_name = tables[i].strip()
            for j in range(1, len(segments), 2):
                company = segments[j].split("   ")[0].strip()
                segment = segments[j+1]
                if re.search("no prox[y|ies]", segment):
                    continue

                try:
                    header, content = re.split("-{15,}", segment)
                except:
                    logging.warning(f"Bad type for {company} in {file_name}.")
                    logging.info(segment)
                    continue

                content = drop_empty(content.split('\n'))
                head = {}
                data = []
                column_header_lines = 0
                for key, value in head_pattern.items():
                    try:
                        head[key] = re.search(value, header).group(0).strip()
                    except:
                        pass
                for line in content:
                    if line.startswith("Prop."):
                        cnames, cpos = get_columns(line)
                        column_header_lines += 1

                    elif line.startswith("    ") and column_header_lines < 2:
                        if len(data) > 0:
                            for k in range(len(cpos) - 1):
                                add = line[cpos[k]:cpos[k+1]].strip()
                                if add:
                                    data[-1][cnames[k]] +=  (" " + add)
                        else:
                            for k in range(len(cpos) - 1):
                                add = line[cpos[k]:cpos[k+1]].strip()
                                if add:
                                    cnames[k] +=  (" " + add)
                                column_header_lines += 1
                    elif line.startswith("    "):
                        continue
                    else:
                        row = {}
                        for l in range(len(cpos) - 1):
                            row[cnames[l]] = line[cpos[l]:cpos[l+1]].strip()
                        data.append(row)
                df = pd.DataFrame(data)
                if len(df) > 0:
                    df.drop(df.columns[0], axis=1, inplace=True)
                    df.replace("", np.nan, inplace=True)
                    for key, value in head.items():
                        df[key] = value
                    df['fund_name'] = fund_name
                    df['company'] = company
                    df['fund_company'] = link_table.loc[n,'fund_company']
                    dfs.append(df)
    results = pd.concat(dfs,sort=False, ignore_index=True)
    logging.info(f"Results shape {results.shape}")
    return results