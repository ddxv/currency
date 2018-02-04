import fromsheet
import tosheet
import numpy as np
import pandas as pd
import requests
import mysql_prices



def stock_ticker_hist(x):
    stock_api_url = f'https://api.iextrading.com/1.0/stock/{x}/chart/1y'
    headers = {'Content-Type': 'text/csv'}
    r = requests.get(stock_api_url)
    r = pd.DataFrame.from_records(r.json())
    r['date'] = pd.to_datetime(r['date'])
    return r


def top_ticker_list(x):
    cmc_api_url = f'https://api.coinmarketcap.com/v1/ticker/?limit={x}'
    headers = {'Content-Type': 'application/json'}
    r = requests.get(cmc_api_url, headers)
    r = pd.DataFrame.from_records(r.json())
    return r


def make_tables(my_df, principal_df, stock):
    #my_df = tdf.copy()
    #principal_df = tdf_principal_df.copy()
    my_sd = min(my_df['Date'].tolist())
    coin_list = list(set(my_df['Currency']))
    coin_df = mysql_prices.get_prices_usd_day(coin_list, my_sd)
    coin_df['date'] = pd.to_datetime(coin_df['date'])
    coin_df = coin_df[coin_df.date >= my_sd]
    my_df = pd.merge(coin_df, my_df, how = "left", left_on = ['date', 'currency'], right_on = ['Date', 'Currency']).fillna(0)
    my_df = my_df.sort_values(['date'])
    #Make the cointoken sum and USD principal cumsum
    my_df['cumsum'] = my_df.groupby(['currency'])['Transfer'].cumsum()
    my_df['Pcumsum'] = my_df.groupby(['currency'])['pUSD'].cumsum()
    #Calculate my USDValue per day
    my_df['USDVal'] = my_df['mean'] * my_df['cumsum']
    my_df['Return'] = my_df['USDVal'] - my_df['Pcumsum']
    my_df['ROI'] = my_df['Return'] / my_df['Pcumsum']
    pdf = my_df.pivot_table(index = ['date'], columns ='currency', values = ['USDVal', 'Return', 'ROI'])
    #Seperate out each nested column    
    my_df = pdf['USDVal'].copy()
    returns = pdf['Return'].reset_index()
    roi = pdf['ROI'].reset_index()
    #Get Total Daily values
    my_df['TotalValue'] = my_df[coin_list].sum(axis = 1)
    my_df['Principal'] = pd.merge(my_df, principal_df, how = "outer", right_index = True, left_index = True).fillna(0)['pUSD'].cumsum()
    my_df = my_df.reset_index()
    my_df['Return'] = my_df['TotalValue'] - my_df['Principal']
    #my_df['tmp'] = my_df['Principal'] * (.08) * (1 / 365)
    my_df = pd.merge(my_df, stock[['date', 'changePercent']], how = 'left', left_on = 'date', right_on = 'date').fillna(0)
    my_df['tmp'] = my_df.Principal * (my_df.changePercent/100)
    my_df['Return @VOO'] = my_df['tmp'].cumsum()
    my_df['Total @VOO'] = my_df['Principal'] + my_df['Return @VOO']
    my_df = my_df.drop(['tmp'], axis = 1)
    #Rearrange, GS graphs farther to the right columns on top
    cols = [col for col in my_df if col not in ['TotalValue','Return']] + ['Return'] + ['TotalValue']
    my_df = my_df[cols]
    my_df_totals = my_df[['date', 'Principal', 'Return @VOO', 'Total @VOO', 'Return', 'TotalValue']]
    my_df = my_df.drop(['Principal', 'Return @VOO', 'Total @VOO', 'Return', 'TotalValue'], axis = 1)
    #Remove NaN for GoogleSheets
    returns = returns.replace(np.nan, 0)
    roi = roi.replace(np.nan, 0)
    return(my_df, my_df_totals, returns, roi)


#Google Sheet names, Worksheet = Main Worksheet, Sheet = tabs in sheet
my_spreadsheet = "housechart"
my_worksheet = "Trans"

#Transaction df
tdf = fromsheet.pull_worksheet(my_spreadsheet, my_worksheet, None)

voo = stock_ticker_hist('voo')


#Fix column types
tdf['Date'] = pd.to_datetime(tdf['Date'])
tdf['pUSD'] = pd.to_numeric(tdf['pUSD']).fillna(0)
tdf['Transfer'] = pd.to_numeric(tdf['Transfer']).fillna(0)
tdf['Amount'] = pd.to_numeric(tdf['Amount'])

idf = tdf[tdf['type'] == 'index'].reset_index(drop=True)
index_coins = list(set(idf.Currency.tolist() + idf.Bought.tolist()))

#Group by total USD, used later to calculate total spend
tdf_principal_df = tdf[['Date', 'pUSD']].groupby(['Date']).sum()

#Start Date
mysd = min(tdf['Date'].tolist())

#Create several cumulative sums of currencies and USD
tdf['cumsum'] = tdf.groupby(['Currency'])['Transfer'].cumsum()
tdf['UScum'] = tdf.groupby(['Currency'])['pUSD'].cumsum()
tdf['lastcum'] = tdf.groupby(['Currency'])['cumsum'].shift(1)
tdf['poc'] = np.where(tdf['Transfer'] < 0, (tdf['Transfer'] / tdf['lastcum']).fillna(0), 0)

tg = tdf.groupby(['Currency'])
grouplist = []
for name, group in tg:
    print(name)
    new = [group['pUSD'].values[0]]
    pval = [0]
    for i in range(1, len(group.index)):
        pval.append(group.poc.values[i] * new[i - 1])
        new.append(group.UScum.values[i] + sum(pval[0:i+1]))
    group.loc[:, 'pval'] = pval.copy()
    grouplist.append(group)


pvaldf = pd.concat(grouplist).sort_index()

new = pvaldf[pvaldf['Bought'] != ""][['Bought', 'Date', 'Amount', 'pval']]
new.columns = ['Currency','Date','Transfer','pUSD']

tdf = pd.concat([pvaldf, new]).fillna(0)
tdf['pUSD'] = tdf['pUSD'].abs()
tdf['Transfer'] = pd.to_numeric(tdf['Transfer'])
tdf = tdf.groupby(['Currency', 'Date'])['pUSD', 'poc', 'pval', 'Transfer'].sum().reset_index()
tdf['pUSD'] = tdf['pUSD'] + tdf['pval']



#Make List of Traded Coins
all_coins = list(set(tdf['Currency']))




#Update MySQL
less_mins = True
mysql_prices.update_prices_usd_minute(less_mins)
mysql_prices.update_prices_usd_day(all_coins, 2)




#Get ByDay Prices
df = mysql_prices.get_prices_usd_day(all_coins, mysd)
df['date'] = pd.to_datetime(df['date'])

# Calculate Price paid, move to google sheet?
idf['p'] = idf.merge(df[['date', 'currency', 'mean']], how = 'left', left_on = ['Date', 'Currency'], right_on = ['date', 'currency'])[['mean']]
idf.pUSD = idf.p * idf.Transfer * -1
idf = idf[['Bought', 'Date', 'Amount', 'pUSD']]
idf.columns = ['Currency','Date','Transfer','pUSD']
idf_principal_df = idf[['Date', 'pUSD']].groupby(['Date']).sum()


top_mc = top_ticker_list(100)
top_mc = top_mc[['available_supply', 'id', 'market_cap_usd', 'percent_change_7d', 'price_usd', 'symbol']]
top_mc = top_mc[top_mc.symbol != 'USDT']
top_mc.loc[top_mc['symbol'] == 'MIOTA', 'symbol'] = 'IOT'
tosheet.insert_df(top_mc, my_spreadsheet, 'coinmarketcap', 0)


index_daily, index_totals, index_returns, index_roi = make_tables(idf, idf_principal_df, voo)
print("Inserting Indexes")
tosheet.insert_df(index_daily, my_spreadsheet, 'indexDaily', 0)
tosheet.insert_df(index_totals, my_spreadsheet, 'indexTotals', 0)
tosheet.insert_df(index_returns, my_spreadsheet, 'indexReturns', 0)
tosheet.insert_df(index_roi, my_spreadsheet, 'indexROI', 0)


tdf_daily, tdf_totals, tdf_returns, tdf_roi = make_tables(tdf, tdf_principal_df, voo)
print("Inserting Totals")
tosheet.insert_df(tdf_daily, my_spreadsheet, 'Daily', 0)
tosheet.insert_df(tdf_roi, my_spreadsheet, 'ROI', 0)
tosheet.insert_df(tdf_totals, my_spreadsheet, 'DailyTotals', 0)
tosheet.insert_df(tdf_returns, my_spreadsheet, 'Returns', 0)


cur_est_cry_bals = tdf.groupby(['Currency'])['Transfer'].sum().reset_index()
tosheet.insert_df(cur_est_cry_bals, my_spreadsheet, 'EstBals', 0)




