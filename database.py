"""
File:           database.py
Version:        0.1
Author:         Marlowe Zhong
Creation Date:  Monday, May 4th 2020, 7:30:29 pm
-----
Last Modified:  Tuesday, May 5th 2020, 3:49:33 pm
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
        self.cursor.execute("BEGIN TRANSACTION")
        for index, row in df.iterrows():
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

    def clear(self, table="vote_record"):
        self.cursor.execute('''DELETE * FROM table''')

    def close(self):
        self.conn.close()


if __name__ == "__main__":
    conn = sqlite3.connect("npx_vote.db")

    c = conn.cursor()

    # Create table
    c.execute('''CREATE TABLE vote_record
                (cusip text, company_name text, ticker text, fund_company text, parent_fund_company text,
                meeting_date text, meeting_type text, proposal text,
                sponsor text, vote text)''')