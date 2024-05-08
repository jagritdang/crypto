import asyncio.log
from pybit import unified_trading
from pymexc import spot, futures
import pandas as pd
import time
import asyncio
import json
from okx import *
import logging
import datetime
import csv
from binance.websocket.um_futures.websocket_client import UMFuturesWebsocketClient
import os

def write_csv():
    global pymexc_ask, pymexc_bid, okx_ask, okx_bid, pybit_ask, pybit_bid, binance_ask, binance_bid
    with open('spread.csv', 'a', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow([datetime.datetime.now(),pybit_bid-okx_ask, okx_bid -pybit_ask, pybit_bid-pymexc_ask, pymexc_bid - pybit_ask, pybit_bid-binance_ask, binance_bid -pybit_ask,  okx_bid-pybit_ask, pybit_bid - okx_ask,  okx_bid-pymexc_ask, pymexc_bid-okx_ask, okx_bid-binance_ask, binance_bid- okx_ask, pymexc_bid-pybit_ask, pybit_bid - pymexc_ask, pymexc_bid-okx_ask, okx_bid-pymexc_ask,pymexc_bid-binance_ask, binance_bid-pymexc_ask, binance_bid-pybit_ask,pybit_bid-binance_ask, binance_bid-okx_ask,okx_bid-binance_ask, binance_bid-pymexc_ask, pymexc_bid-binance_ask])



ht = []
ys1 = []
ys2 = []
ys3 = []
logger = logging.getLogger('Spread')
formatter = logging.Formatter('%(asctime)s-%(message)s')
file_handler = logging.FileHandler('spread.log')
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

pybit_ask = None
pybit_bid = None
okx_ask = None
okx_bid = None
pymexc_ask = None
pymexc_bid = None
binance_ask = None
binance_bid = None


xs = unified_trading.WebSocket(
    testnet=False,
    channel_type="linear",
)

def handle_message(message):
    try:
        global pybit_ask, pybit_bid, okx_ask, okx_bid, pymexc_bid, pymexc_ask
        data = message["data"]
        pybit_close = float(data["markPrice"])
        pybit_ask = float(data["ask1Price"])
        pybit_bid = float(data["bid1Price"])
        # print('[pybit]PYBIT'+'Symbol:'+'BTCUSDT' + 'Ask:'+ str(pybit_ask)+ 'Bid:'+ str(pybit_bid))
        # if pybit_ask is not None and pybit_bid is not None and okx_ask is not None and okx_bid is not None:
            # logger.info(f"[pybit]PYBIT_ASK-OKX_BID:{pybit_ask-okx_bid} || OKX_ASK-PYBIT_BID:{okx_ask-pybit_bid} ||")
    except Exception as e:
        print("PYBIT API- Failure",e)

async def pybit_data():
    xs = unified_trading.WebSocket(
    testnet=False,
    channel_type="linear",
)
    xs.ticker_stream( "BTCUSDT", handle_message)


def ws_handler(s):
    global okx_ask, okx_bid, pybit_ask, pybit_bid, pymexc_ask, pymexc_bid
    data = json.loads(s)
    
    if("event" in data):
        if(data["event"] == "subscribe"):
            print("Subscribed")
            return
        if(data["event"] == "unsubscribe"):
            print("Unsubscribed")
            return
    
    if("arg" in data and "channel" in data["arg"]):
        channel = data["arg"]["channel"]
        symbol = data["arg"]["instId"]
        if(channel == "tickers"):
            ticker = data["data"][0]
            okx_close = float(ticker['last'])
            okx_ask = float(ticker['askPx'])
            okx_bid = float(ticker['bidPx'])
            # print("[OKX] Symbol:"+ symbol +" Ask:"+ str(okx_ask) +" Bid:"+ str(okx_bid) +"Ltp:" + str(okx_close) )
            # if pybit_ask is not None and pybit_bid is not None and okx_ask is not None and okx_bid is not None:
                # logger.info(f"[okx]PYBIT_ASK-OKX_BID:{pybit_ask-okx_bid} || OKX_ASK-PYBIT_BID:{okx_ask-pybit_bid} ||")
        else:
            print(f"[UNKNOWN] {text}")

async def tickers():
    ws = OkxSocketClient()
    await ws.public.start()
    await ws.public.subscribe([{'channel': "tickers", 'instId': "BTC-USDT-SWAP"}], callback=ws_handler)


def handle_message_pymexc(message):
    global okx_ask, okx_bid, pymexc_ask, pymexc_bid, pybit_bid, pybit_ask, binance_ask, binance_bid
    pymexc_ask = float(message['d']['a'])
    pymexc_bid = float(message['d']['b'])
    # print(f"[pyemxc]Symbol:BTC_USDT| Ask:{str(pymexc_ask)} | Bid:{str(pymexc_bid)}")
    

async def pym():
    spot_client = spot.HTTP()
    client_websocket = spot.WebSocket()
    spot_client.exchange_info()
    client_websocket.book_ticker(handle_message_pymexc, 'BTCUSDT')

def binance_handle_message(_, message):
    global pybit_ask, pybit_bid, okx_ask, okx_bid, pymexc_ask, pymexc_bid, binance_ask, binance_bid
    message = json.loads(message)
    symbol = message['s']
    binance_ask = float(message['a'][0][0])
    binance_bid = float(message['b'][0][0])
    # print(f'[binance]SYMBOL:{symbol} | ASK:{str(binance_ask)} | BID:{str(binance_bid)}')
    if pymexc_ask is not None and pymexc_bid is not None and okx_ask is not None and okx_bid is not None and pybit_ask is not None and pybit_bid is not None and binance_ask is not None and binance_bid is not None:
        write_csv()

# async def binance_data():
my_client = UMFuturesWebsocketClient(on_message=binance_handle_message)
my_client.partial_book_depth(symbol='BTCUSDT')

        
async def main():
    await asyncio.gather(pybit_data(), tickers(),pym())

asyncio.run(main())

while True:
    time.sleep(0.5)