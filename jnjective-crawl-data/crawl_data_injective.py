import json
import requests
import pandas as pd
import os, sys
from datetime import datetime, timedelta
from sqlalchemy import create_engine, Table, MetaData, insert
from dotenv import load_dotenv
import sqlite3

load_dotenv()

API_URL = os.getenv("API_URL")

DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")

DATABASE_URL = f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
engine = create_engine(DATABASE_URL)
conn = sqlite3.connect("db.sqlite")
cur = conn.cursor()

# cur.execute("""
#     CREATE TABLE IF NOT EXISTS coin_prices (
#         id INTEGER PRIMARY KEY AUTOINCREMENT,
#         timestamp INTEGER NOT NULL,
#         datetime TEXT NOT NULL,
#         base_currency TEXT NOT NULL,
#         target_currency TEXT NOT NULL,
#         last_price REAL NOT NULL,
#         base_volume REAL NOT NULL,
#         target_volume REAL NOT NULL,
#         bid REAL NOT NULL,
#         ask REAL NOT NULL,
#         high REAL NOT NULL,
#         low REAL NOT NULL
#     )
# """)
# conn.commit()

# with open("product.json", "r") as file:
#     product_list = json.load(file)

class InjectiveBot:
    def __init__(self, connection):
        self.connection = connection

    def get_next_round_timestamp(self, interval: int):
        now = datetime.now()
        
        if interval == 3600: 
            next_time = (now + timedelta(hours=1)).replace(minute=0, second=0, microsecond=0)
        elif interval == 86400:  
            next_time = (now + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
        else:
            raise ValueError("Interval wrong!")

        return int(next_time.timestamp()), next_time.strftime("%Y-%m-%d %H:%M:%S")

    def get_candles(self, interval: int = 3600):
        response = requests.get(API_URL)
        
        if response.status_code == 200:
            data = response.json()
            
            if not isinstance(data, list) or len(data) == 0:
                print("Error API")
                return
            
            df = pd.DataFrame(data)

            required_columns = {'base_currency', 'target_currency', 'last_price', 'base_volume',
                                'target_volume', 'bid', 'ask', 'high', 'low'}
            if not required_columns.issubset(df.columns):
                print("Error API")
                return
            
            timestamp, datetime_str = self.get_next_round_timestamp(interval)
            df['timestamp'] = timestamp
            df['datetime'] = datetime_str
            
            self.save_to_db(df)
        else:
            print(f"Error API {response.status_code}")

    def save_to_db(self, df: pd.DataFrame):
        try:
            data_tuples = df[['timestamp', 'datetime', 'base_currency', 'target_currency', 'last_price',
                              'base_volume', 'target_volume', 'bid', 'ask', 'high', 'low']].values.tolist()
            
            self.connection.executemany("""
                INSERT INTO coin_prices (timestamp, datetime, base_currency, target_currency, last_price, 
                                         base_volume, target_volume, bid, ask, high, low)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, data_tuples)
            
            self.connection.commit()
            print(f"Data write to database (Timestamp: {df['timestamp'][0]})")
        except Exception as e:
            print(f"Error to write database {e}")

    def run(self, interval: int = 3600):
        self.get_candles(interval)


if __name__ == "__main__":
    bot = InjectiveBot(conn)
    
    try:
        if len(sys.argv) > 1:
            command = sys.argv[1]
            if command == "hourly":
                bot.run(3600)
            elif command == "daily":
                bot.run(86400)
            
        else:
            bot.run()  
    except Exception as e:
        print(f"Lỗi: {e}")

    conn.close()

