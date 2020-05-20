import urllib
import requests
from bs4 import BeautifulSoup
import pandas as pd
import os
import logging


def download(fund_family, output_path="text/", info_path="/user/zz2654/python/npx_parse/Edgar list2018.xlsx", info_crawl=None,
             info_sheet_name=0, year=2018):
    logging.info(f"Start to download {fund_family.strip()}.")
    info = pd.read_excel(info_path, sheet_name=info_sheet_name)
    info["parentfundcompany"] = info["parentfundcompany"].str.lower()
    if info_crawl is not None:
        info = info[(info["parentfundcompany"] == fund_family) & (info.crawl == info_crawl)]
    else:
        info = info[(info["parentfundcompany"] == fund_family)]
    logging.info(f"There are {len(info)} funds in {fund_family.strip()}.")
    # print(info.shape)

    link_table = []

    for company_name in info.Company:
        page_size = 80
        main = "https://www.sec.gov"
        search_url = (
            main
            + '/cgi-bin/srch-edgar?text=n-px+company-name%3D"{}"&first={}&last={}'.format(
                urllib.parse.quote_plus(company_name), year, year
            )
        )
        req = requests.get(search_url)
        bs = BeautifulSoup(req.content, "lxml")
        try:
            total_page = max(1, len(bs.find("center").find_all("a")))
        except:
            total_page = 1
        for page in range(total_page):
            if page != 0:
                req = requests.get(
                    search_url
                    + "&start={}&count={}".format(page_size * page + 1, page_size)
                )
                bs = BeautifulSoup(req.content, "lxml")
            table = bs.find("a", href="/").findNext("table").find_all("tr")
            if len(table) < 2:
                logging.warning(f"{company_name} not found!")
                continue
            elif len(table) > 2:
                logging.warning(f"Found more than 1 funds after searching {company_name}")
            for tr in table[1:]:
                item = {}
                rows = tr.find_all("td")
                item["fund_company"] = rows[1].get_text()
                item["file_link"] = main + rows[2].find("a")["href"]
                item["filing_date"] = rows[4].get_text()
                link_table.append(item)

    link_table = pd.DataFrame(link_table).drop_duplicates().reset_index()
    logging.info(f"Obtain {len(link_table)} funds after searching.")

    if not os.path.exists(output_path):
        os.makedirs(output_path)

    for i in range(len(link_table)):
        link = link_table.loc[i, "file_link"]
        file_name = link.rsplit("/", 1)[-1]
        link_table.loc[i, "file_name"] = file_name
        if file_name in os.listdir(output_path):
            continue
        text = requests.get(link)
        with open(output_path + file_name, "w") as f:
            f.write(text.text)

    logging.info(f"{fund_family.strip()} downloading completed.")

    return link_table

if __name__ == "__main__":
    download('gabelli')