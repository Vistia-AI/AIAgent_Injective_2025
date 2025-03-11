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


# ================================================
import asyncio
import os
import uuid
from decimal import Decimal

import dotenv
from grpc import RpcError

from pyinjective.async_client import AsyncClient
from pyinjective.constant import GAS_FEE_BUFFER_AMOUNT, GAS_PRICE
from pyinjective.core.network import Network
from pyinjective.transaction import Transaction
from pyinjective.wallet import PrivateKey


async def main() -> None:
    dotenv.load_dotenv()
    configured_private_key = os.getenv("PRIVATE_KEY")

    # select network: local, testnet, mainnet
    network = Network.testnet()

    # initialize grpc client
    client = AsyncClient(network)
    composer = await client.composer()
    await client.sync_timeout_height()

    # load account
    priv_key = PrivateKey.from_hex(configured_private_key)
    pub_key = priv_key.to_public_key()
    address = pub_key.to_address()
    await client.fetch_account(address.to_acc_bech32())
    subaccount_id = address.get_subaccount_id(index=0)

    # prepare trade info
    market_id = "0x0611780ba69656949525013d947713300f56c37b6175e02f26bffa495c3208fe"
    fee_recipient = "inj1hkhdaj2a2clmq5jq6mspsggqs32vynpk228q3r"

    # prepare tx msg
    msg = composer.msg_create_spot_market_order(
        market_id=market_id,
        sender=address.to_acc_bech32(),
        subaccount_id=subaccount_id,
        fee_recipient=fee_recipient,
        price=Decimal("10.522"),
        quantity=Decimal("0.01"),
        order_type="BUY",
        cid=str(uuid.uuid4()),
    )

    # build sim tx
    tx = (
        Transaction()
        .with_messages(msg)
        .with_sequence(client.get_sequence())
        .with_account_num(client.get_number())
        .with_chain_id(network.chain_id)
    )
    sim_sign_doc = tx.get_sign_doc(pub_key)
    sim_sig = priv_key.sign(sim_sign_doc.SerializeToString())
    sim_tx_raw_bytes = tx.get_tx_data(sim_sig, pub_key)

    # simulate tx
    try:
        sim_res = await client.simulate(sim_tx_raw_bytes)
    except RpcError as ex:
        print(ex)
        return

    sim_res_msg = sim_res["result"]["msgResponses"]
    print("---Simulation Response---")
    print(sim_res_msg)

    # build tx
    gas_price = GAS_PRICE
    gas_limit = int(sim_res["gasInfo"]["gasUsed"]) + GAS_FEE_BUFFER_AMOUNT  # add buffer for gas fee computation
    gas_fee = "{:.18f}".format((gas_price * gas_limit) / pow(10, 18)).rstrip("0")
    fee = [
        composer.coin(
            amount=gas_price * gas_limit,
            denom=network.fee_denom,
        )
    ]
    tx = tx.with_gas(gas_limit).with_fee(fee).with_memo("").with_timeout_height(client.timeout_height)
    sign_doc = tx.get_sign_doc(pub_key)
    sig = priv_key.sign(sign_doc.SerializeToString())
    tx_raw_bytes = tx.get_tx_data(sig, pub_key)

    # broadcast tx: send_tx_async_mode, send_tx_sync_mode, send_tx_block_mode
    res = await client.broadcast_tx_sync_mode(tx_raw_bytes)
    print("---Transaction Response---")
    print(res)
    print("gas wanted: {}".format(gas_limit))
    print("gas fee: {} INJ".format(gas_fee))


if __name__ == "__main__":
    asyncio.get_event_loop().run_until_complete(main())