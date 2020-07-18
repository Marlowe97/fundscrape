import os
import scrape
import sys
sys.path.append("E:/Marlowe/RA/npx_parse/")
from fundscrape.template import base, two_lines, up_down, blue, left_right
from fundscrape.scrape import download
from fundscrape.database import NpxVote
import logging
import pandas as pd
from tqdm import tqdm

os.chdir("E:/Marlowe/RA/npx_parse/")


# ff = "columbia"
# ff = ff.lower()
# text_root = f"results_2019/{ff}/text/"
# if not os.path.isdir(f"results_2019/{ff}/"):
#     os.makedirs(text_root)
# link_table = scrape.download(ff, output_path=text_root, info_path="E:\\Marlowe\\RA\\npx_parse\\Edgar list2019.xlsx", info_crawl=None, year=2019)

# results = base.parse(link_table, text_root=text_root)

# results.to_csv(f"results_2019/{ff}/columbia_npx.csv")

# category = pd.read_excel("Edgar list2018_v2.xlsx", sheet_name="category")

# logging.basicConfig(filename='fundscrape/npx_leftright.log', filemode='w', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# dataIO = NpxVote()

# ff_list = category.loc[category.category=="leftright", "fund_family"]
# for ff in tqdm(ff_list[2:]):
#     ff = ff.lower()
#     text_root = f"results_2018/{ff}/text/"
#     if not os.path.isdir(f"results_2018/{ff}/"):
#         os.makedirs(text_root)
#     link_table = download(ff, output_path=text_root, info_path="E:/Marlowe/RA/npx_parse/Edgar list2018_v2.xlsx")

#     results = left_right.parse(link_table, text_root=text_root)
#     results['securityID'] = results['securityID'].str.split(' ').str[1]
#     results['parent_fund_company'] = ff
#     columns = ["securityID", "company", "ticker", "fund_company", "parent_fund_company",
#                "meetingDate", "meetingType", "Description", "Proponent", "Vote Cast"]
#     logging.info("Started to insert into database.")
#     # dataIO.delete_fund_family(ff)
#     dataIO.insert_dataframe(results[columns])

# dataIO.close()