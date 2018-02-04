import pandas as pd
import gdax
import requests
import json
import os
import time
from bittrex.bittrex import Bittrex, API_V2_0
from binance.client import Client as binClient
from kucoin.client import Client as kuClient


#Tool to search list of dictionaries
def my_find(lst, key, value):
    for i, dic in enumerate(lst):
        if dic[key] == value:
            return dic



def initialize_exchange(my_client, my_ex):
    auth = json.load(open(os.path.expanduser('~/auth/exchanges.auth')))
    auth = my_find(auth, 'exchange', my_ex)
    key = auth['key']
    secret = auth['secret']
    if my_ex == 'gdax':
        passphrase = auth['passphrase']
        client = my_client.AuthenticatedClient(key, secret, passphrase)
    else:
        client = my_client(key, secret)
    return(client)

def strf_float(decimal_places, my_float):
    my_float = f"%.{decimal_places}f" % my_float
    my_float = str(my_float)
    return(my_float)

def top_ticker_list(x):
    cmc_api_url = f'https://api.coinmarketcap.com/v1/ticker/?limit={x}'
    headers = {'Content-Type': 'application/json'}
    r = requests.get(cmc_api_url, headers)
    r = pd.DataFrame.from_records(r.json()).symbol.tolist()
    return r

def gdax_send(target_btc_address, transfer_amt):
    result = gdax_client.crypto_withdraw(transfer_amt, 'BTC', target_btc_address)
    return(result)



def get_transfer_amt(btc_bal, targets):
    bittrex_num_targs = len(targets)
    transfer_amt = bittrex_num_targs * amt_btc_buy
    transfer_amt = transfer_amt + transfer_amt * .003
    transfer_amt = transfer_amt - btc_bal
    transfer_amt = strf_float(8, transfer_amt)
    return(transfer_amt)

# Vars
top_num = 20
amt_btc_buy = .0015

# Create Clients
ku_client = initialize_exchange(kuClient, 'ku')
bin_client = initialize_exchange(binClient, 'binance')
bit_client = initialize_exchange(Bittrex, 'bittrex')
gdax_client = initialize_exchange(gdax, 'gdax')

pub_bittrex = Bittrex(None, None)  # or defaulting to v1.1 as Bittrex(None, None)


binance_btc_address = bin_client.get_deposit_address(asset='BTC')['address']
bittrex_btc_address = bit_client.get_deposit_address('BTC')['result']['Address']

binance_prices = bin_client.get_all_tickers()
binance_prices = pd.DataFrame(binance_prices)


target_tickers = top_ticker_list(top_num)
# Remove BCC (BitConnect) from CoinMarketCap list
target_tickers = [x for x in target_tickers if x != 'BCC']
target_tickers = [x for x in target_tickers if x != 'USDT']
# Bittrex uses BCC for BitCoin Cash
target_tickers = [x.replace('BCH', 'BCC') for x in target_tickers]
target_tickers = [x.replace('MIOTA', 'IOTA') for x in target_tickers]


# Bittrex
bittrex_markets = pub_bittrex.get_markets()
bittrex_markets = pd.DataFrame.from_dict(bittrex_markets['result'])
bittrex_targets = bittrex_markets['MarketCurrency'].tolist()
bittrex_targets = list(set(target_tickers) - (set(target_tickers) - set(bittrex_targets)))
bittrex_market_names = ['BTC-' + x for x in bittrex_targets if x != 'BTC']
remaining_targets = set(target_tickers) - set(bittrex_targets)

# Binance 
binance_market_names = [x + 'BTC' for x in remaining_targets]
binance_target_markets = binance_prices[binance_prices['symbol'].isin(binance_market_names)]['symbol'].tolist()
binance_target_tickers = [x.replace('BTC', '') for x in binance_target_markets]
remaining_targets = set(remaining_targets) - set(binance_target_tickers)

# Kucoin
ku_currencies = pd.DataFrame(ku_client.get_trading_symbols())
ku_target_markets = ku_currencies[(ku_currencies['coinType'].isin(remaining_targets)) & (ku_currencies['coinTypePair'] == 'BTC')].symbol.tolist()
ku_target_tickers = ku_currencies[(ku_currencies['coinType'].isin(remaining_targets)) & (ku_currencies['coinTypePair'] == 'BTC')].coinType.tolist()
remaining_targets = set(remaining_targets) - set(ku_target_tickers)
len(remaining_targets)


# Send BTC Binance
binance_btc_bal = float(bin_client.get_asset_balance('BTC')['free'])

binance_transfer_amt = get_transfer_amt(binance_btc_bal, binance_target_markets)

result = gdax_send(binance_btc_address, binance_transfer_amt)


bittrex_btc_bal = bit_client.get_balance('BTC')['result']['Balance']


# Transfer Ku
ku_btc_bal = ku_client.get_coin_balance('BTC')['balance']
ku_transfer_amt = get_transfer_amt(ku_btc_bal, ku_target_markets)
ku_btc_address = ku_client.get_deposit_address('BTC')['address']

result = gdax_send(ku_btc_address, ku_transfer_amt)




# Buy From Markets

for tc in bittrex_market_names:
    print(tc)
    result = pub_bittrex.get_marketsummary(tc)['result']
    cur_price = result[0]['Last']
    (amt_btc_buy / cur_price) > bittrex_markets[bittrex_markets['MarketName'] == tc]['MinTradeSize']
    buy_quantity = amt_btc_buy / cur_price
    print(buy_quantity)
    print(cur_price)
    r = bit_client.buy_limit(market = tc, quantity = buy_quantity, rate = cur_price)
    r
    time.sleep(10)


#binance_target_markets = list(set(binance_target_markets) - set([tc]))


for tc in binance_target_markets:
    print(tc)
    sym_inf = bin_client.get_symbol_info(tc)
    alltickers = bin_client.get_all_tickers()
    cur_price = float(my_find(alltickers, 'symbol', tc)['price'])
    #(amt_btc_buy / float(cur_price)) > float(my_find(sym_inf['filters'], 'filterType', 'LOT_SIZE')['minQty'])
    sym_inf = bin_client.get_symbol_info(tc)
    step_size = my_find(sym_inf['filters'], 'filterType', 'LOT_SIZE')['stepSize']
    buy_quantity = amt_btc_buy / cur_price
    step_dec = int(abs(decimal.Decimal(step_size.strip('0')).as_tuple().exponent))
    buy_quantity = f"%.{step_dec}f" % buy_quantity
    print(buy_quantity)
    result = bin_client.order_market_buy(symbol = tc, quantity = buy_quantity)
    print(result)
    time.sleep(10)



for tc in ku_target_markets:


last_price = ku_client.get_tick(tc)['lastDealPrice']
buy_quantity = amt_btc_buy / last_price
buy_quantity = strf_float(6, buy_quantity)
last_price = strf_float(6, last_price)


result = ku_client.create_buy_order(tc, last_price, buy_quantity)
print(result)


