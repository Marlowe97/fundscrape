"""
File:           database.py
Version:        0.1
Author:         Marlowe Zhong
Creation Date:  Monday, May 4th 2020, 7:30:29 pm
-----
Last Modified:  Thursday, May 28th 2020, 5:50:50 pm
Modified By:    Marlowe Zhong (marlowezhong@gmail.com)
"""


import sqlite3
import os
import logging
import pandas as pd

class NpxVote:

    def __init__(self, database="npx_vote.db"):
        self.conn = sqlite3.connect(database)
        self.cursor = self.conn.cursor()

    def insert_line(self, values, table="vote_record"):
        try:
            values_quote = ", ".join(["'" + str(value).replace("'", "''") + "'" for value in values])
            self.cursor.execute(f'''INSERT INTO {table} VALUES ({values_quote})''')
            # logging.info(f"Record inserted successfully into {table} table!")
        except:
            logging.warning(f"Failed to insert into SQL table. {values_quote}")

    def insert_dataframe(self, df, table="vote_record"):
        for chunk in range(0, len(df), 50000):
            self.cursor.execute("BEGIN TRANSACTION")
            for index, row in df.iloc[chunk:chunk+50000,:].iterrows():
                self.insert_line(row, table)
            self.cursor.execute("COMMIT")
        logging.info(f"Insert into {table} successfully.")

    def drop_duplicates(self, table="vote_record"):
        sql_query = f'''
        WITH CTE AS
        (
        SELECT *,ROW_NUMBER() OVER (PARTITION BY rollNumber ORDER BY rollNumber) AS RN
        FROM {table}
        )

        DELETE FROM CTE WHERE RN<>1
        '''
        logging.info("Duplicates dropped.")
        self.cursor.execute(sql_query)
        self.conn.commit()

    def delete_fund_family(self, fund_family):
        sql_query = f'''
        DELETE FROM vote_record WHERE parent_fund_company='{fund_family}'
        '''
        self.cursor.execute(sql_query)
        self.conn.commit()
        logging.info(f"{fund_family} deleted.")

    def clear(self, table="vote_record"):
        self.cursor.execute('''DELETE * FROM table''')
        self.conn.commit()

    def close(self):
        self.conn.close()


if __name__ == "__main__":
    conn = sqlite3.connect("npx_vote_2018.db")

    c = conn.cursor()

    # Create table
    c.execute('''CREATE TABLE vote_record
                (cusip text, company_name text, ticker text, fund_name text, fund_company text, parent_fund_company text,
                meeting_date text, meeting_type text, proposal text,
                sponsor text, vote text, link text)''')