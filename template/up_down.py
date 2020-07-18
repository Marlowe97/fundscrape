"""
File:           up_down.py
Version:        0.1
Author:         Marlowe Zhong
Creation Date:  Friday, May 8th 2020, 6:29:01 pm
-----
Last Modified:  Thursday, May 28th 2020, 12:05:04 am
Modified By:    Marlowe Zhong (marlowezhong@gmail.com)
"""


import re
import os
import requests
import logging
import pandas as pd
import numpy as np
from bs4 import BeautifulSoup
from io import StringIO
from fundscrape.template.base import normal_size

head_pattern = {'ticker': r"Ticker\s*",
                'securityID': r"Security ID:\s*",
                'meetingDate': r"Meeting Date\s*",
                'meetingType': r"Meeting Type\s*",
                'meetingStatus': r"Meeting Status\s*",
                'country': r"Country of Trade\s*"}

def drop_empty(xx):
    return [ii for ii in xx if ii.replace("=","").strip()]

def preprocess(text, file_name):
    # remove the last few lines
    try:
        if re.search('SIGNATURES', text):
            text = re.search('(.|\n)*(?=SIGNATURES)', text).group(0)
        elif re.search('Pursuant\s*to\s*the\s*requirements', text):
            text = re.search('(.|\n)*(?=Pursuant\s*to\s*the\s*requirements)', text).group(0)
        else:
            logging.warning(f"Ending not found in {file_name}!")
            return None
    except:
        logging.warning(f"Ending not found in {file_name}!")
        return None
    # use beautifulsoup to remove some html tags
    bs = BeautifulSoup(text, 'lxml')
    # replace non-breaking space in Latin1 (ISO 8859-1) and chr(160) with space
    text = bs.get_text().replace("\xa0"," ")
    text = re.sub(r"Registrant :([^\n]+)", "", text)
    return text

def generate_dataframe(data, head_info, fund_name, company, fund_company, link):
    '''
    Generate the output dataframe from the data and information in the table head
    '''
    df = pd.DataFrame(data)
    # add the information in the table head
    df.replace("", np.nan, inplace=True)
    for key, value in head_info.items():
        df[key] = value
    df['fund_name'] = fund_name
    df['company'] = company
    df['fund_company'] = fund_company
    df['link'] = link
    return df

def parse(link_table, text_root='text/', first_separate=r"Fund Name :([^\n]+)\n", second_separate=r"\_{10,}"):
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
        # process the text
        text = preprocess(text, file_name)
        if text:
            tables = re.split(first_separate, text)
            logging.info(f"There are {(len(tables)-1)//2} sub-funds in this file.")
            if ((len(tables)-1)//2 == 0) and normal_size(text_root + file_name):
                logging.warning(f"Something wrong with {file_name}")

            for i in range(1,len(tables), 2):
                if re.search(second_separate, tables[i+1]):
                    # segments = [x.strip() for x in re.split(second_separate, tables[i+1]) if x]
                    segments = drop_empty(re.split(second_separate, tables[i+1]))
                else:
                    continue
                fund_name = tables[i].strip()
                for segment in segments[1:]:
                    if "no proxy voting" in segment:
                        continue
                    segment = drop_empty(segment.split('\n'))
                    # at least 4 lines above
                    if len(segment) < 4:
                        continue
                    header = True
                    header_value = False
                    colname = False
                    head_info = {}
                    data = []
                    company = segment[0]
                    for line in segment[1:]:
                        if header and (not colname):
                            if not header_value:
                                hpos = {}
                                count = 0
                                for key, value in head_pattern.items():
                                    try:
                                        hpos[key] = (re.search(value, line).span())
                                        count += 1
                                    except:
                                        pass
                                if count == 0:
                                    colname = True
                                    header = False
                                else:
                                    header_value = True
                                    continue
                            else:
                                for key,value in hpos.items():
                                    head_info[key] = line[value[0]:value[1]]
                                header_value = False
                                continue
                        if colname:
                            cnames = []
                            cpos = []
                            for word in line.split("  "):
                                name = word.strip()
                                if name:
                                    cnames.append(name)
                                    cpos.append(re.search(name, line).span()[0])
                            cpos.append(None)
                            colname = False

                        elif line.startswith("    "):
                            if len(data) > 0:
                                for i in range(len(cpos) - 1):
                                    add = line[cpos[i]:cpos[i+1]].strip()
                                    if add:
                                        data[-1][cnames[i]] +=  (" " + add)
                        else:
                            row = {}
                            for i in range(len(cpos) - 1):
                                row[cnames[i]] = line[cpos[i]:cpos[i+1]].strip()
                            data.append(row)

                    dfs.append(generate_dataframe(data, head_info, fund_name, company, 
                                                  fund_company=link_table.loc[n, 'fund_company'],
                                                  link=link_table.loc[n, 'file_link']))

    results = pd.concat(dfs, sort=False, ignore_index=True)
    logging.info(f"Results shape {results.shape}")
    return results