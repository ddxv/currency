import numpy as np
import pandas as pd
import gdax
#import requests
import json
import os
#import time
from bittrex.bittrex import Bittrex, API_V2_0
from binance.client import Client as binClient
from kucoin.client import Client as kuClient
#import fromsheet
#import decimal
#

def pivot_currency(trans_df):
    trans_df['date'] = pd.to_datetime(trans_df['date'])
    trans_df['bought_amt'] = pd.to_numeric(trans_df['bought_amt'])
    trans_df['sold_amt'] = pd.to_numeric(trans_df['sold_amt'])
    bought = trans_df.pivot_table(index = 'date', columns=['bought'], values = ['bought_amt'], aggfunc = 'sum').fillna(0)['bought_amt']
    sold = trans_df.pivot_table(index = 'date', columns=['sold'], values = ['sold_amt'], aggfunc = 'sum').fillna(0)['sold_amt']
    balances = bought.add(sold, fill_value = 0)
    return(balances)



def split_buy_sell(df, split_char, buy_name, buy_on_left):
    if buy_on_left == True:
        b = 0
        s = 1
    else:
        b = 1
        s = 0
    df['bought'] = np.where(df.side == buy_name, df.product_id.str.split(split_char).str[b], df.product_id.str.split('-').str[s])
    df['sold'] = np.where(df.side == 'sell', df.product_id.str.split(split_char).str[b], df.product_id.str.split('-').str[s])
    return df

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



#### Calculate Balances ###


# Clients
bin_client = initialize_exchange(binClient, 'binance')
bit_client = initialize_exchange(Bittrex, 'bittrex')
gdax_client = initialize_exchange(gdax, 'gdax')
bittrex_pub = Bittrex(None, None)  # or defaulting to v1.1 as Bittrex(None, None)
gdax_pub = gdax.PublicClient()


# Bittrex
bit_hist = bit_client.get_order_history()
bit_hist = pd.DataFrame(bit_hist['result'])
bit_hist['Closed'] = pd.to_datetime(bit_hist['Closed'])
bit_hist = bit_hist.rename(columns = {'Closed': 'date', 'Exchange': 'product_id', 'OrderType': 'side', 'Quantity': 'bought_amt'})


bit_hist = split_buy_sell(bit_hist, '-', 'LIMIT_BUY', False)

bit_hist['sold_amt'] = bit_hist['bought_amt'] * bit_hist['PricePerUnit'] + bit_hist['Commission'] * -1



# KuCoin
ku_client = initialize_exchange(kuClient, 'ku')



ku_hist = ku_client.get_dealt_orders()
ku_hist = pd.DataFrame(ku_hist['datas'])

ku_hist = ku_hist.rename(columns = {'createdAt': 'date', 'amount': 'bought_amt', 'coinType': 'bought', 'coinTypePair': 'sold', 'dealDirection': 'side', 'dealPrice': 'price'})

ku_hist['sold_amt'] = (ku_hist['bought_amt'] * ku_hist['price'] + ku_hist['fee']) * -1
ku_hist['date'] = pd.to_datetime(ku_hist['date'], unit = 'ms')



# Binance
bin_acc = bin_client.get_account()
bin_acc = pd.DataFrame(bin_acc['balances'])
bin_bals = bin_acc[(pd.to_numeric(bin_acc.free) > 0) | (pd.to_numeric(bin_acc.locked) > 0)]

bin_coins = bin_bals.asset.tolist()

temp_bin_hist = []
for c in bin_coins:
    btchist = []
    bnbhist = []
    print(c)
    try:
        cn = c + 'BTC'
        btchist = bin_client.get_my_trades(symbol = cn)
        for item in btchist:
            item.update({'sold': 'BTC'})
            item.update({'bought': c})
    except:
        pass
    try:
        cn = c + 'BNB'
        bnbhist = bin_client.get_my_trades(symbol = cn)
        for item in bnbhist:
            item.update({'sold': 'BNB'})
            item.update({'bought': c})
    except:
        pass
    bn = btchist + bnbhist
    #bn = pd.DataFrame(btchist + bnbhist)
    temp_bin_hist.append(bn)


flat_list = [item for sublist in temp_bin_hist for item in sublist]
bin_hist = pd.DataFrame(flat_list)

bin_hist = bin_hist.rename(columns = {'time': 'date', 'commission': 'fee', 'qty': 'bought_amt'})

bin_hist['date'] = pd.to_datetime(bin_hist.date, unit = 'ms')
bin_hist['bought_amt'] = pd.to_numeric(bin_hist['bought_amt'])
bin_hist['fee'] = pd.to_numeric(bin_hist['fee'])
bin_hist['price'] = pd.to_numeric(bin_hist['price'])

bin_hist['sold_amt'] = bin_hist['price'] * bin_hist['bought_amt'] * -1




gdax_fills = gdax_client.get_fills()
flat_list = [item for sublist in gdax_fills for item in sublist]
gdax_hist = pd.DataFrame(flat_list)
gdax_hist = split_buy_sell(gdax_hist, '-', 'buy', True)

gdax_hist[pd.to_numeric(gdax_hist.fee) > 0].head()

gdax_hist['usd_volume'] = pd.to_numeric(gdax_hist['size']) * pd.to_numeric(gdax_hist['price'])

gdax_hist['sold_amt'] = np.where(gdax_hist.side == 'sell', pd.to_numeric(gdax_hist['size']) * -1, (pd.to_numeric(gdax_hist['price']) * pd.to_numeric(gdax_hist['size']) + pd.to_numeric(gdax_hist['fee'])) * -1)



gdax_hist['bought_amt'] = np.where(gdax_hist.side == 'sell', gdax_hist['usd_volume'], gdax_hist['size'])


#gdax_hist['usd_volume'] = np.where(gdax_hist.side == 'buy', gdax_hist['usd_volume'], pd.to_numeric(gdax_hist['size']) * pd.to_numeric(gdax_hist['price']))


gdax_final = gdax_hist[['created_at', 'trade_id', 'product_id', 'bought', 'sold', 'bought_amt', 'sold_amt']].copy()


gdax_final = gdax_final.sort_values('created_at')

gdax_final['bought_amt'] = pd.to_numeric(gdax_final['bought_amt'])
gdax_final['sold_amt'] = pd.to_numeric(gdax_final['sold_amt'])

gdax_final = gdax_final.rename(columns = {'created_at': 'date'})


man_hist = fromsheet.pull_worksheet('housechart', 'ManTrans', None)

man_hist['date'] = pd.to_datetime(man_hist['date'])
gdax_final['date'] = pd.to_datetime(gdax_final['date'])



bal_df = pd.concat([gdax_final, man_hist, ku_hist, bit_hist, bin_hist])
bal_df = pivot_currency(bal_df)
bal_df = bal_df.sort_index(axis = 0, ascending = True)
bal_df = bal_df.cumsum()
bal_df = bal_df.reset_index()

bal_df[['date', 'BTC']].head(10)


tosheet.insert_df(bal_df, 'housechart', 'auto_est_bals', 0)

gdax_final[gdax_final['sold_amt'] > 0 ]

gdax_final










my_accounts = gdax_client.get_accounts()
gdax_bal = pd.DataFrame(my_accounts)
gdax_bal = gdax_bal.set_index('currency')
gdax_bal['balance'] = pd.to_numeric(gdax_bal['balance'])

bit_bal = bit_client.get_balances()['result']
bit_bal = pd.DataFrame(bit_bal)
bit_bal = bit_bal.rename(columns= {'Currency': 'currency', 'Balance':'balance'})
bit_bal = bit_bal.set_index('currency')
bit_bal['balance'] = pd.to_numeric(bit_bal['balance'])

ku_bal = ku_client.get_all_balances()
ku_bal = pd.DataFrame(ku_bal)
ku_bal = ku_bal[ku_bal['balance'] > 0]
ku_bal = ku_bal.rename(columns= {'coinType': 'currency'})
ku_bal = ku_bal.set_index('currency')
ku_bal['balance'] = pd.to_numeric(ku_bal['balance'])

bin_acc = bin_client.get_account()
bin_acc = pd.DataFrame(bin_acc['balances'])
bin_bal = bin_acc[(pd.to_numeric(bin_acc.free) > 0) | (pd.to_numeric(bin_acc.locked) > 0)]
bin_bal = bin_bal.rename(columns= {'asset': 'currency', 'free':'balance'})
bin_bal = bin_bal.set_index('currency')
bin_bal['balance'] = pd.to_numeric(bin_bal['balance'])

combined_bal = bin_bal[['balance']].add(gdax_bal[['balance']], fill_value = 0)
combined_bal = combined_bal[['balance']].add(bit_bal[['balance']], fill_value = 0)
combined_bal = combined_bal[['balance']].add(ku_bal[['balance']], fill_value = 0)


estbals = fromsheet.pull_worksheet('housechart', 'EstBals', None)

estbals = estbals.set_index('Currency')


comp=estbals.merge(combined_bal, how = 'outer', right_index = True, left_index = True)

comp

comp['Transfer'] - comp['balance']






# GDAX
my_accounts = gdax_client.get_accounts()

gdax_bal = pd.DataFrame(my_accounts)

import copy

#gdax_acc_hist = gdax_client.get_account_history('6ab7d398-1591-4746-8432-2ace9f6aa16d')
#gdax_acc_hist = gdax_client.get_account_history('5bba226b-5d2c-4173-9272-fead74a1a8f1')


hist_list = []
sold_list = []
for i in my_accounts:
    my_id = i['id']
    my_cur = i['currency']
    gdax_acc_hist = gdax_client.get_account_history(my_id)
    gdax_acc_hist = [item for sublist in gdax_acc_hist for item in sublist]
    for d in gdax_acc_hist:
        if d['type'] == 'transfer':
            d['cur'] = my_cur
            d = {**d, **d.pop('details', None)}
            hist_list.append(d)

transfer_hist = pd.DataFrame(hist_list)

sold_list = pd.DataFrame(sold_list)


acc_hist['created_at'] = pd.to_datetime(acc_hist['created_at'])


