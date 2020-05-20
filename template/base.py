"""
File:           base.py
Version:        0.1
Author:         Marlowe Zhong
Creation Date:  Friday, May 8th 2020, 6:29:01 pm
-----
Last Modified:  Wednesday, May 20th 2020, 6:03:47 pm
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


# Record the regular expression for each data field in the table head
head_pattern = {'ticker': r"(?<=Ticker:)\s*\w*",
                'securityID': r"(?<=Security ID:)\s*\w*",
                'meetingDate': r"(?<=Meeting Date:)\s*\w{3} \d{2}, \d{4}",
                'meetingType': r"(?<=Meeting Type:).*",
                'recordDate': r"(?<=Record Date:)\s*\w{3} \d{2}, \d{4}"}

## auxilary functions
def normal_size(path, threshold = 30):
    fsize = os.path.getsize(path)
    fsize = fsize/float(1024)
    return fsize > threshold

def drop_empty(xx):
    '''Drop empty strings in the list'''
    return [ii for ii in xx if ii.strip()]

def preprocess(text, file_name):
    '''
    Preprocess the text file
    '''
    # remove the last few lines
    try:
        if re.search(r'END N(?:-)?PX[-| ]REPORT', text):
            text = re.search(r'((?:.|\n)*)END N(?:-)?PX[-| ]REPORT', text).group(1)
        elif re.search(r'SIGNATURES', text):
            text = re.search(r'((?:.|\n)*)SIGNATURES', text).group(1)
        elif re.search(r'</PRE>', text):
            text = re.search(r'((?:.|\n)*)</PRE>', text).group(1)
        elif re.search(r'<PAGE>', text):
            text = re.search(r'((?:.|\n)*)<PAGE>', text).group(1)
        else:
            logging.warning(f"Ending not found in {file_name}!")
            return None
    except:
        logging.warning(f"Ending not found in {file_name}!")
        return None

    # use beautifulsoup to remove some html tags
    bs = BeautifulSoup(text,'lxml')
    # replace non-breaking space in Latin1 (ISO 8859-1) and chr(160) with space
    text = bs.get_text().replace("\xa0"," ")
    # fit fund names into a single line
    text = re.sub(r"\s*={3,}\n={3,}\s*", " ", text)
    return text

def get_columns(line):
    '''
    Get the column names and record the position of each column
    '''
    column_names = []
    column_pos = []
    # split the line using double spaces
    for word in line.split("  "):
        name = word.strip()
        if name:
            column_names.append(name)
            # record the start position of this column
            column_pos.append(re.search(name, line).span()[0])
    column_pos.append(None)
    return column_names, column_pos

def generate_dataframe(data, head_info, fund_name, company, fund_company):
    '''
    Generate the output dataframe from the data and information in the table head
    '''
    df = pd.DataFrame(data)
    # drop the index column
    if '#' in df.columns:
        df.drop('#', axis=1, inplace=True)
        # add the information in the table head
        df.replace("", np.nan, inplace=True)
        for key, value in head_info.items():
            df[key] = value
        df['fund_name'] = fund_name
        df['company'] = company
        df['fund_company'] = fund_company
        return df
    else:
        return None

def parse(link_table, text_root='text/', first_separate = r"={1,}([^=\n]+)={3,}",
          second_separate=r"\-{10,}"):
    dfs = []
    # iterate through the downloaded N-PX files
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
            # split all the tables for the fund companies
            tables = re.split(first_separate, text)
            logging.info(f"There are {(len(tables)-1)//2} sub-funds in this file.")
            if ((len(tables)-1)//2 == 0) and normal_size(text_root + file_name):
                logging.warning(f"Something wrong with {file_name}")

            for i in range(1,len(tables),2):
                # split each table into segments showing votes for different target companies
                if re.search(second_separate, tables[i+1]):
                    # segments = [x.strip() for x in re.split(second_separate, tables[i+1]) if x]
                    segments = drop_empty(re.split(second_separate, tables[i+1]))
                else:
                    continue
                # get the name of the fund
                fund_name = tables[i].strip()
                for segment in segments:
                    if re.search("no prox[y|ies]", segment):
                        break
                    # split the segment into lines
                    segment = drop_empty(segment.split('\n'))
                    # require the segment have at least 4 lines
                    if len(segment) < 4:
                        continue

                    is_head_info = True
                    head_info = {}
                    data = []
                    company = segment[0]

                    # iterate through each line
                    for line in segment[1:]:

                        # if this line is the column names
                        if line.startswith("#"):
                            cnames, cpos = get_columns(line)

                            if is_head_info == True:
                                is_head_info = False
                            # if we already have column headers
                            # save the data we already have and use the new header
                            else:
                                dfs.append(generate_dataframe(data, head_info, fund_name, company, fund_company=link_table.loc[n,'fund_company']))
                                data = []

                        # if this line is in the head information part
                        elif is_head_info:
                            # search the patterns and save available information
                            for key, value in head_pattern.items():
                                try:
                                    head_info[key] = re.search(value, line).group(0).strip()
                                except:
                                    pass
                        # if the line starts with spaces, combine with the line above
                        elif line.startswith("    "):
                            aligned = True
                            if len(data) > 0:
                                # check each column to add the data fields
                                for i in range(len(cpos) - 1):
                                    value = line[cpos[i]:cpos[i+1]]
                                    if value:
                                        if aligned:
                                            add = value.strip()
                                            if add:
                                                data[-1][cnames[i]] +=  (" " + add)
                                        else:
                                            add_prev = value.strip()
                                            if add_prev:
                                                data[-1][cnames[i-1]] += add_prev

                                        if value.endswith(" ") or value.endswith('\n'):
                                            aligned = True
                                        else:
                                            aligned = False

                        elif line.startswith("====="):
                            break
                        # finally, if a line in the table
                        # use the position of columns to get the text
                        else:
                            row = {}
                            aligned = True
                            for i in range(len(cpos) - 1):
                                value = line[cpos[i]:cpos[i+1]]
                                if aligned:
                                    row[cnames[i]] = value.strip()
                                else:
                                    values = re.split(r"\s{2,}", value)
                                    if len(values) == 2:
                                        row[cnames[i-1]] = row[cnames[i-1]] + values[0]
                                        row[cnames[i]] = values[1]
                                    else:
                                        logging.warning(f"Bad Type. {values}")
                                if value.endswith(" ") or value.endswith('\n'):
                                    aligned = True
                                else:
                                    aligned = False
                            data.append(row)
                    dfs.append(generate_dataframe(data, head_info, fund_name, company, fund_company=link_table.loc[n,'fund_company']))
    results = pd.concat(dfs,sort=False, ignore_index=True)
    logging.info(f"Results shape {results.shape}")
    return results