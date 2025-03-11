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

import sqlite3
con = sqlite3.connect("sqlite.db")
cur = con.cursor()

# from . import config
import time, json, requests
import sys, os, logging
import numpy as np
sys.path.append(os.path.abspath('..'))
from config.injective_chain import RPC, SWAP_ROUNTER_ADDRESS, SWAP_ROUNTER_ABI, FACTORY_ADDRESS, FACTORY_ABI, WALLET, token_info, market_info

# logging.basicConfig(filename="./logs/test.log",
#                     filemode='a',
#                     format='%(message)s',
#                     level=logging.INFO)

network = Network.testnet()

# initialize grpc client
client = AsyncClient(network)

class DEXSwapBot():
    def __init__(self, name:str, gateway, swap_router, swap_factory, wallet:tuple, pair:str, token_info:dict, native_token='INJI'):
        self.name = name
        self.invest_balance = [0,0]
        self.total_invest = 0  # lock token amount in wei
        self.walletAddress = wallet[0]
        self.privateKey = wallet[1]
        self.token_info = token_info
        self.pair = pair # try get more pair[0] token from trade pair
        self.native_token = native_token
        self.logger = logging.getLogger("name")  # Logger
        self.gas_limit = 200000
    
    def _wait_for_receipt(self, txn_hash):
        receipt = None
        # print('Waiting for receipt')
        for i in range(0, 120):
            try:
                receipt = self.gateway.eth.get_transaction_receipt(txn_hash)
            except TransactionNotFound:
                time.sleep(0.5)
                continue
            if receipt:
                return receipt

    async def swap(self, pair:list=['sW','BOO'], amount_in:int=1000000000000000000, amount_out_min:int=0):
        if amount_in < 0:
            raise Exception('Invalid amount_in')
        if amount_out_min <= 0:
            amount_out_min = 1
        
        sell_token, buy_token = pair
        try:
            sell_token_address, sell_token_abi = self.token_info.get(sell_token)
            buy_token_address, buy_token_abi = self.token_info.get(buy_token)
            nt_add, nt_abi = self.token_info[self.native_token]
        except Exception:
            raise Exception('Buy or sell token not found!')
        
        composer = await client.composer()
        await client.sync_timeout_height()
        
        priv_key = PrivateKey.from_hex(self.privateKey)
        pub_key = priv_key.to_public_key()
        address = pub_key.to_address()

        subaccount_id = address.get_subaccount_id(index=0)
        market_id = market_info.get(f"{sell_token}_{buy_token}")
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

        return res

    def get_token_decimal(self, decimal):
        decimal = int("1" + str("0" * decimal))
        decimals_dict = {"wei": 1,
                        "kwei": 1000,
                        "babbage": 1000,
                        "femtoether": 1000,
                        "mwei": 1000000,
                        "lovelace": 1000000,
                        "picoether": 1000000,
                        "gwei": 1000000000,
                        "shannon": 1000000000,
                        "nanoether": 1000000000,
                        "nano": 1000000000,
                        "szabo": 1000000000000,
                        "microether": 1000000000000,
                        "micro": 1000000000000,
                        "finney": 1000000000000000,
                        "milliether": 1000000000000000,
                        "milli": 1000000000000000,
                        "ether": 1000000000000000000,
                        "kether": 1000000000000000000000,
                        "grand": 1000000000000000000000,
                        "mether": 1000000000000000000000000,
                        "gether": 1000000000000000000000000000,
                        "tether": 1000000000000000000000000000000}

        # list out keys and values separately
        key_list = list(decimals_dict.keys())
        val_list = list(decimals_dict.values())

        # print key with val 100
        position = val_list.index(decimal)
        return key_list[position]

    def estimate(self, t_path, amount_in_wei, native_token='INJI'):
        pass
        return None


    def get_invest_value(self):
        p, a = self.estimate(self.pair[::-1], self.invest_balance[1])
        value2 = a[-1]
        value = self.invest_balance[0] + value2
        return value

    def getROI(self):
        # return profit / invest
        if self.total_invest == 0:
            return 0        
        return (self.get_invest_value()-self.total_invest) / self.total_invest * 100
    
    def get_trade_decision(self) -> tuple:
        # set your trade strategy here
        # get decision from vistiaAI 
        order_pair = None
        amount = 0
        res = requests.get("https://api.vistia.co/api/v2/al-trade/top-over-sold/v2.2?heatMapType=rsi7&interval=5m")
        if res.status_code == 200:
            for s in res.json():
                if s['symbol'] == ''.join(self.pair): # buy phase
                    order_pair = self.pair
                    amount = int(self.invest_balance[0] * 0.3)
                    break
        if order_pair is None:
            res = requests.get("https://api.vistia.co/api/v2/al-trade/top-over-bought/v2.2?heatMapType=rsi7&interval=5m")
            if res.status_code == 200:
                for s in res.json():
                    if s['symbol'] == ''.join(self.pair[::-1]): # sell phase
                        order_pair = self.pair[::-1]
                        amount = int(self.invest_balance[1] * 0.8)
                        break
        return order_pair, amount

    def run(self):
        order_pair, amount = self.get_trade_decision()

        if order_pair is not None and amount > 0:
            result = self.swap(pair=order_pair, amount_in=amount, amount_out_min=1)
            self.logger.info(result)
            amount_in = amount

            amount_out = result['amount_out']            
            print(f"{self.name} trade {amount_in} {order_pair[0]} for {amount_out} {order_pair[1]} \ntx: {result['txn_hash']}")


class BotManager():
    def __init__(self, bots: list=[], db_connect=None, 
                 wallet=["",""], gateway=None, 
                 swap_router=None, swap_factory=None, token_info=None):
        self.bots = bots
        self.db_connect = db_connect
        self.wallet = wallet
        self.gateway = gateway
        self.swap_router = swap_router
        self.swap_factory = swap_factory
        self.token_info = token_info

    def run(self):
        for bot in self.bots:
            bot.run()
            self.save_bot_state(bot.name)
            
    def add_bot(self, bot):
        self.bots.append(bot)

    def remove_bot(self, bot):
        self.bots.remove(bot)

    def get_bot(self, name):
        for bot in self.bots:
            if bot.name == name:
                return bot
        return None

    def get_all_bot(self):
        return self.bots

    def get_bot_state(self, name):
        bot = self.get_bot(name)
        if bot:
            return {
                'name': bot.name,
                'pair': bot.pair,
                'invest_balance': bot.invest_balance,
                'total_invest': bot.total_invest,
                'roi': bot.getROI()
            }
        return None

    def get_all_bot_state(self):
        states = []
        for bot in self.bots:
            states.append({
                'name': bot.name,
                'pair': bot.pair,
                'invest_balance': bot.invest_balance,
                'total_invest': bot.total_invest,
                'roi': bot.getROI()
            })
        return states

    def get_all_bot_name(self):
        names = []
        for bot in self.bots:
            names.append(bot.name)
        return names
    
    def save_bot_state(self, bot_name=None):
        cur = con.cursor()
        bots = self.bots if bot_name is None else [self.get_bot(bot_name)]
        ts = int(time.time())
        # res = cur.execute("""CREATE TABLE IF NOT EXISTS bot_report(time, name, address,token_1,token_2,amount_1,amount_2,invert,roi)""")
        for bot in bots:
            # res = cur.execute(f"""
            #     INSERT INTO bot_report (time,name,address,token_1,token_2,amount_1,amount_2,invert,roi)
            #     VALUES (
            #         {ts},
            #         '{bot.name}',
            #         '{bot.walletAddress}',
            #         '{bot.pair[0]}', 
            #         '{bot.pair[1]}', 
            #         {bot.invest_balance[0]},
            #         {bot.invest_balance[1]},
            #         {bot.total_invest},
            #         {bot.getROI():.2f}
            #     )
            # """)
            pass
        # con.commit()

    def load_bot(self, name):
        cur = self.db_connect.cursor()
        data = []
    
        res = cur.execute(f"""SELECT * FROM bot_report WHERE name = "{name}" order by time desc limit 1""")
        state = res.fetchone()
        cur.close()

        bot = self.get_bot(name)
        if bot is None:
            bot = DEXSwapBot(
                name=name,
                gateway=self.gateway,
                wallet=WALLET, 
                swap_router=self.swap_router, 
                swap_factory=self.swap_factory, 
                pair=[state[3], state[4]], 
                token_info=self.token_info)
            bot.invest_balance = [int(state[5]), int(state[6])]
            bot.total_invest = int(state[7])
            self.bots.append(bot)            
        else:
            bot.pair = [state[3], state[4]]
            bot.invest_balance = [int(state[5]), int(state[6])]
            bot.total_invest = int(state[7])
        return bot

    def allocate_funding(self):
        # bots_name = []
        rois = []
        invests = [] 
        for bot in self.bots:
            # bots_name.append(bot.name)
            rois.append(bot.getROI())
            invests.append(bot.total_invest)
        t_i = sum(invests)
        print("Total invest: ", t_i)
        total_return = sum(rois) + len(rois)*100
        re_alloc_invest = [(100+p)*t_i / total_return for p in rois]
        print("realloc invest:")
        for i, bot in enumerate(self.bots):
            change = bot.total_invest - re_alloc_invest[i]
            if change>0:
                # print("withdraw")
                bot.withdraw(change)
            else:
                # print("deposite")
                bot.deposite(-change)
            print(f" {i}. Bot {bot.name} funding: {bot.total_invest}") 



if __name__ == "__main__":
    command = "run" 
    print("""All command:
 - list
 - load {bot_name}
 - run
 - report
 - reallocate """)

    con = sqlite3.connect("sqlite.db")
    bm = BotManager(
        db_connect=con,
        wallet=wallet,
        token_info=token_info
    )
    while (command.lower() != "exit"):
        print("____________________________________________")
        command = input().strip()
        if command == "list":
            print("""All command:
 - list
 - load {bot_name}
 - run
 - report
 - reallocate """)

        elif command[:4].lower() == "load":
            bot_name = command[4:].strip()
            try:
                bm.load_bot(bot_name)
                print("Load success")
            except:
                print("Load fail") 
        # elif command.lower() == "run":
        #     for bot in bm.bots:
        #         bot.run()
        elif command.lower() == "report":
            total_invest = 0
            bots = []
            for bot in bm.bots:
                funding = bot.total_invest
                total_invest += funding
                bots.append([bot.name, funding, bot.getROI()])
            print(f"Total funding: {total_invest}")
            for bot in bots:
                print(f" - Bot {bot[0]} funding: {bot[1]:.6f} - ROI: {bot[2]}")
        elif command.lower() == "reallocate":
            bm.allocate_funding()
        else:
            print("Start trade:")
            bm.run()


