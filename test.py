import os
import scrape
import sys
sys.path.append("E:/Marlowe/RA/npx_parse/")
from fundscrape.template import base, two_lines, up_down, blue
import logging
import pandas as pd
from tqdm import tqdm

os.chdir("E:/Marlowe/RA/npx_parse/")

ff = "columbia"
ff = ff.lower()
text_root = f"results_2019/{ff}/text/"
if not os.path.isdir(f"results_2019/{ff}/"):
    os.makedirs(text_root)
link_table = scrape.download(ff, output_path=text_root, info_path="E:\\Marlowe\\RA\\npx_parse\\Edgar list2019.xlsx", info_crawl=None, year=2019)

results = base.parse(link_table, text_root=text_root)

results.to_csv(f"results_2019/{ff}/columbia_npx.csv")