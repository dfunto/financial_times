import sys
import os
import sqlite3
import requests
import pandas as pd

from dotenv import load_dotenv
from datetime import date, timedelta

load_dotenv()


class DatabaseHandler():
    
    def __init__(self) -> None:

        db_path = 'financial.db'

        # Create database if not exists
        if os.path.isfile(db_path):
            self.conn = sqlite3.connect(db_path)
        else:
            self.conn = sqlite3.connect(db_path)
            self.__init_db()

    def __init_db(self):

        # Create rates_hist table
        cursor = self.conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS rates_hist (
                base VARCHAR(5) NOT NULL,
                date_ref DATE NOT NULL,
                currency VARCHAR(5) NOT NULL,
                rate FLOAT,
                PRIMARY KEY(base, date_ref, currency)
            );
        """)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        if self.conn is not None:
            self.conn.close()

    def insert_rates(self, df: object) -> None:
        df.to_sql(
            "rates_hist", 
            con=self.conn,
            if_exists="append"
        )

    def delete_rates(self, date_ref: date) -> None:
        cursor = self.conn.cursor()
        cursor.execute(""" 
            DELETE 
            FROM rates_hist
            WHERE date_ref = ?
        """, [date_ref]
        )

    def query_avg_rates(self, begin_date: date, end_date: date, base: str, currency: str) -> object:
        query = """
            SELECT base, currency, MIN(date_ref) begin_date, MAX(date_ref) end_date, AVG(rate) avg_rate
            FROM rates_hist
            WHERE currency = ? AND 
                  date_ref BETWEEN ? AND ? AND
                  base = ?
            GROUP BY base, currency
        """
        df = pd.read_sql(
            query,
            con=self.conn,
            params=[currency, begin_date, end_date, base]
        )
        return df

    def query_hist_rates(self, begin_date: date, end_date: date, base: str) -> object:
        query = """
            SELECT 
                base, 
                currency, 
                date_ref, 
                rate
            FROM rates_hist
            WHERE date_ref BETWEEN ? AND ? AND
                  base = ?
            ORDER BY date_ref
        """
        df = pd.read_sql(
            query,
            con=self.conn,
            params=[begin_date, end_date, base]
        )
        return df


class PipelineFixer():

    def __init__(self) -> None:
        self.base_url = "http://data.fixer.io/api/"
        self.token = os.getenv("FIXER_API_TOKEN")

        if self.token is None:
            print("Missing FIXER_API_TOKEN environment variable")
            sys.exit()

    def load_hist_rate(self, date_ref: date, base_currency: str) -> object:

        rates_url = "{base_url}/{date}?access_key={token}&base={base}".format(
            base_url=self.base_url,
            date=date_ref.strftime("%Y-%m-%d"),
            token=self.token,
            base=base_currency
        )

        response = requests.get(rates_url)
        if response.status_code == 200:
            rates_data = response.json()
            if rates_data["success"]:
                df = pd.DataFrame(rates_data)
                df.drop(['success', 'timestamp', 'historical'], axis='columns', inplace=True)
                df.index.name = 'currency'
                df.rename(columns={"date": "date_ref", "rates": "rate"}, inplace=True)
                return df

        return None

    def run(self, begin_date: date, end_date: date, base_currency: str) -> None:

        with DatabaseHandler() as db_handler:
            
            cur_date = begin_date
            while(cur_date <= end_date):

                df = self.load_hist_rate(cur_date, base_currency)
                if df is not None:
                    db_handler.delete_rates(cur_date)
                    db_handler.insert_rates(df)

                cur_date = cur_date + timedelta(days=1)


def load_hist_rates(begin_date: date, end_date: date, base_currency: str = "EUR"):
    
    pipeline = PipelineFixer()
    
    try:
        pipeline.run(begin_date, end_date, base_currency)
        print("Data loaded successfully")

    except Exception as e:
        print(f"Error running pipeline: {str(e)}")

def disp_hist_rates(begin_date: date, end_date: date, base_currency: str = "EUR"):
    with DatabaseHandler() as db_handler:
        df = db_handler.query_hist_rates(
            begin_date,
            end_date,
            base_currency
        )

        print(df.pivot(index='currency',columns='date_ref', values=['rate']).to_markdown())

def disp_avg_rates(begin_date: date, end_date: date, currency: str, base_currency: str = "EUR"):
    with DatabaseHandler() as db_handler:
        df = db_handler.query_avg_rates(
            begin_date,
            end_date,
            base_currency,
            currency
        )
        print(df.to_markdown(index=False))







