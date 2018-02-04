import numpy as np
import pandas as pd
import datetime
import os

#define a conversion function for the native timestamps in the csv file
def dateparse (time_in_secs):
        return datetime.datetime.fromtimestamp(float(time_in_secs))

print('Data listing...')

print(os.listdir('/data/bitcoin-historical-data/'))

# read in the data and apply our conversion function, this spits out a DataFrame with the DateTimeIndex already in place
print('Using bitstampUSD_1-min_data...')
data = pd.read_csv('/data/bitcoin-historical-data/bitstampUSD_1-min_data_2012-01-01_to_2017-05-31.csv', parse_dates=True, date_parser=dateparse, index_col=[0])

kdata = pd.read_csv('/data/bitcoin-historical-data/krakenUSD_1-min_data_2014-01-07_to_2017-05-31.csv', parse_dates=True, date_parser=dateparse, index_col=[0])

kdata=clean_df(kdata)

print('Total null open prices: %s' % data['Open'].isnull().sum())


def clean_df(myDF):
    # First thing is to fix the data for bars/candles where there are no trades. 
    # Volume/trades are a single event so fill na's with zeroes for relevant fields...
    myDF['Volume_(BTC)'].fillna(value=0, inplace=True)
    myDF['Volume_(Currency)'].fillna(value=0, inplace=True)
    myDF['Weighted_Price'].fillna(value=0, inplace=True)
    # next we need to fix the OHLC (open high low close) data which is a continuous timeseries so
    # lets fill forwards those values...
    myDF['Open'].fillna(method='ffill', inplace=True)
    myDF['High'].fillna(method='ffill', inplace=True)
    myDF['Low'].fillna(method='ffill', inplace=True)
    myDF['Close'].fillna(method='ffill', inplace=True)
    return myDF


