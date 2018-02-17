import pandas as pd
import gdax
import requests
import json
import os
import time
from bittrex.bittrex import Bittrex, API_V2_0
from binance.client import Client as binClient
from kucoin.client import Client as kuClient
import fromsheet
import decimal

#Tool to search list of dictionaries
def my_find(lst, key, value):
    for i, dic in enumerate(lst):
        if dic[key] == value:
            return dic

#Global variables for available account balances
def get_gdax_bals():
    my_accounts = gdax_client.get_accounts()
    while type(my_accounts) is not list:
        logger.warning("Retrying get_accounts")
        sleep(1)
        my_accounts = ac.get_accounts()
    #gdax_btc_acc = my_find(my_accounts, 'currency', 'BTC')['profile_id']
    #gdax_usd_acc = my_find(my_accounts, 'currency', 'USD')['profile_id']
    gdax_btc_bal = float(my_find(my_accounts, 'currency', 'BTC')['available'])
    gdax_usd_bal = float(my_find(my_accounts, 'currency', 'USD')['available'])
    return gdax_btc_bal, gdax_usd_bal


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


def gdax_price(tick, use_cur):
    cur_combo = '{}-{}'.format(tick, use_cur)
    my_price = gdax_pub.get_product_ticker(product_id = cur_combo)['price']
    my_price = float(my_price)
    return my_price


def gdax_send(target_btc_address, transfer_amt):
    result = gdax_client.crypto_withdraw(transfer_amt, 'BTC', target_btc_address)
    return(result)


def gdax_buy(buy_tick, buy_with, less_limit):
    btc_usd = gdax_price(buy_tick, 'USD')
    eth_btc = gdax_price(buy_tick, buy_with)
    buy_amt_usd = buy_df[buy_df.Symbol == buy_tick]['Final Buy'].values[0]
    tick_buy_amt = buy_amt_usd / btc_usd
    if tick_buy_amt > .008 and tick_buy_amt < .01:
        tick_buy_amt = .01
    tick_buy_amt = strf_float(8, tick_buy_amt)
    buy_price = eth_btc - (eth_btc * limit_percent)
    buy_price = strf_float(4, buy_price)
    print(buy_tick, tick_buy_amt, buy_price)
    result = gdax_client.buy(price = buy_price, #PurchasePrice
            size = tick_buy_amt, #Buy
            product_id = '{}-{}'.format(buy_tick, buy_with),
            time_in_force = 'GTC')
    return result



def bittrex_buy(ts, buy_df):
    print(ts)
    bittrex_market_name = 'BTC-' + ts
    result = bittrex_pub.get_marketsummary(bittrex_market_name)['result']
    cur_price = result[0]['Last']
    amt_btc_buy = buy_df[buy_df['Symbol'] == ts].btc_amt.values[0]
    buy_quantity = amt_btc_buy / cur_price
    min_trade = bittrex_markets[bittrex_markets['MarketName'] == bittrex_market_name]['MinTradeSize'].values[0]
    if buy_quantity > min_trade:
        print(buy_quantity)
        print(cur_price)
        r = bit_client.buy_limit(market = bittrex_market_name, quantity = buy_quantity, rate = cur_price)
        return(r)
    else:
        print("ERROR: MinTradeSize for " + bittrex_market_name)



def binance_buy(tc):
    print(tc)
    sym_inf = bin_client.get_symbol_info(tc)
    alltickers = bin_client.get_all_tickers()
    cur_price = float(my_find(alltickers, 'symbol', tc)['price'])
    sym_inf = bin_client.get_symbol_info(tc)
    step_size = my_find(sym_inf['filters'], 'filterType', 'LOT_SIZE')['stepSize']
    buy_quantity = amt_btc_buy / cur_price
    step_dec = int(abs(decimal.Decimal(step_size.strip('0')).as_tuple().exponent))
    buy_quantity = f"%.{step_dec}f" % buy_quantity
    print(buy_quantity)
    result = bin_client.order_market_buy(symbol = tc, quantity = buy_quantity)
    return(result)



def get_transfer_amt(btc_bal, targets, buy_df):
    buy_total = buy_df[buy_df.Symbol.isin(bittrex_targets)].btc_amt.sum()
    buy_total = buy_total - btc_bal
    transfer_amt = buy_total + (buy_total * .006)
    transfer_amt = strf_float(8, transfer_amt)
    return(transfer_amt)


def get_even_transfer_amt(btc_bal, targets):
    bittrex_num_targs = len(targets)
    transfer_amt = bittrex_num_targs * amt_btc_buy
    transfer_amt = transfer_amt + transfer_amt * .003
    transfer_amt = transfer_amt - btc_bal
    transfer_amt = strf_float(8, transfer_amt)
    return(transfer_amt)


my_spreadsheet = 'housechart'
my_worksheet = 'Rebalance'
df = fromsheet.pull_worksheet(my_spreadsheet, my_worksheet, None)

disp_usd = float(df[0:1].Dispurse.replace('[\$,]', '', regex = True).values[0])

df = df[4:]
df.columns = df.iloc[0]
df = df.drop(4)
df['Final Buy'] = pd.to_numeric(df['Final Buy'].replace('[\$,]', '', regex=True))

buy_df = df.loc[df['Final Buy'] > 0, ['Symbol', 'Final Buy']]
buy_symbol_list = list(buy_df.Symbol)

btc_usd = gdax_price('BTC', 'USD')

buy_df['btc_amt'] = buy_df['Final Buy'] / btc_usd


buy_symbol_list = [x for x in buy_symbol_list if x != 'BTC']





# Vars
top_num = 20
amt_btc_buy = .0015
target_tickers = top_ticker_list(top_num)
# Remove BCC (BitConnect) from CoinMarketCap list
target_tickers = [x for x in target_tickers if x != 'BCC']
target_tickers = [x for x in target_tickers if x != 'USDT']
# Bittrex uses BCC for BitCoin Cash
target_tickers = [x.replace('BCH', 'BCC') for x in target_tickers]
target_tickers = [x.replace('MIOTA', 'IOTA') for x in target_tickers]



# Create Clients
ku_client = initialize_exchange(kuClient, 'ku')
bin_client = initialize_exchange(binClient, 'binance')
bit_client = initialize_exchange(Bittrex, 'bittrex')
gdax_client = initialize_exchange(gdax, 'gdax')
bittrex_pub = Bittrex(None, None)  # or defaulting to v1.1 as Bittrex(None, None)
gdax_pub = gdax.PublicClient()


gdax_btc_bal, gdax_usd_val = get_gdax_bals()


limit_percent = .01


gdax_prod = gdax_pub.get_products()
gdax_prod = pd.DataFrame(gdax_prod)
gdax_prod_list = list(set(gdax_prod.base_currency))

gdax_buy_list = [x for x in buy_symbol_list if x in gdax_prod_list]
gdax_buy_list = [x for x in gdax_buy_list if x != 'BTC']
#gdax_buy_list = [x for x in gdax_buy_list if x != 'ETH']

_gdax_bought = []
for buy_tick in gdax_buy_list:
    my_result = gdax_buy(buy_tick, 'BTC', less_limit = .005)
    _gdax_bought.append(my_result)

# Just commented out for safety for this time, uncomment later
#gdax_bought = pd.DataFrame(_gdax_bought)


gdax_bought_prod = list(gdax_bought.product_id.replace('-BTC','', regex = True))

buy_symbol_list = [x for x in buy_symbol_list if x not in gdax_bought_prod]

target_tickers = buy_symbol_list

binance_btc_address = bin_client.get_deposit_address(asset='BTC')['address']
bittrex_btc_address = bit_client.get_deposit_address('BTC')['result']['Address']

binance_prices = bin_client.get_all_tickers()
binance_prices = pd.DataFrame(binance_prices)

# Bittrex
bittrex_market = bittrex_pub.get_markets()

bittrex_market = pd.DataFrame.from_dict(bittrex_market['result'])
bittrex_targets = bittrex_market['MarketCurrency'].tolist()
bittrex_targets = list(set(target_tickers) - (set(target_tickers) - set(bittrex_targets)))

remaining_targets = set(target_tickers) - set(bittrex_targets)

# Binance 
remaining_targets = [x.replace('IOT', 'IOTA') for x in remaining_targets]
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



bittrex_btc_bal = bit_client.get_balance('BTC')['result']['Balance']
bittrex_btc_bal


bittrex_transfer_amt = get_transfer_amt(bittrex_btc_bal, bittrex_targets, buy_df)

result = gdax_send(bittrex_btc_address, bittrex_transfer_amt)


# Send BTC Binance
binance_btc_bal = float(bin_client.get_asset_balance('BTC')['free'])

binance_transfer_amt = get_transfer_amt(binance_btc_bal, binance_target_markets, buy_df)

result = gdax_send(binance_btc_address, binance_transfer_amt)




# Transfer Ku
ku_btc_bal = ku_client.get_coin_balance('BTC')['balance']
ku_transfer_amt = get_transfer_amt(ku_btc_bal, ku_target_markets)
ku_btc_address = ku_client.get_deposit_address('BTC')['address']

result = gdax_send(ku_btc_address, ku_transfer_amt)




_btr_bought = []
for ts in bittrex_targets:
    result = bittrex_buy(ts, buy_df)
    _btr_bought.append(result)



_btr_hist = []
for x in _btr_bought:
    x['result']['uuid']
    my_order = bit_client.get_order(x['result']['uuid'])
    _btr_hist.append(my_order['result'])

btr_bought = pd.DataFrame.from_dict(_btr_hist)



#binance_target_markets = list(set(binance_target_markets) - set([tc]))

_bin_bought = []
for tc in binance_target_markets:
    result = binance_buy(tc)
    _bin_bought.append(result)

bin_bought = pd.DataFrame(_bin_bought)

bin_bought['price']

gdax_bought

btr_bought

for tc in ku_target_markets:


last_price = ku_client.get_tick(tc)['lastDealPrice']
buy_quantity = amt_btc_buy / last_price
buy_quantity = strf_float(6, buy_quantity)
last_price = strf_float(6, last_price)


result = ku_client.create_buy_order(tc, last_price, buy_quantity)
print(result)



