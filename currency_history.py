from sheets import pullsheet
import os
import crycompare
import numpy as np
import pandas as pd
from joblib import Parallel, delayed
import operator
import datetime
#import mysql.connector
import json
import _mysql

import MySQLdb

from sqlalchemy import create_engine

s=open('/home/bitnami/auth/mysql.auth','r').read()

myUser=json.loads(s)['user']
myPass=json.loads(s)['pass']

engine = create_engine('mysql://@localhost:3306/crypto', echo=False)

#data.to_sql(name='sample_table2', con=engine, if_exists = 'append', index=False)


p = crycompare.Price()

#All Coins List
coinList = p.coinList()

myCoins = sorted(list( coinList['Data'].keys() ))

h = crycompare.History()
#myWS="Crypto Currencies"
#myS="CashFlow"

#df=pullsheet.sheet_df(myWS,myS)
#df['Date']=pd.to_datetime(df['Date'])
#df['Total USD']=df['Total USD'].convert_objects(convert_numeric=True).fillna(0)
#df['Transfer']=df['Transfer'].convert_objects(convert_numeric=True).fillna(0)
#idf=df[['Date','Total USD']].groupby(['Date']).sum()
#flowdf=df[['Currency','Date','Transfer']]
#myCoins=list(set(flowdf['Currency']))
#mysd=min(flowdf['Date'].tolist())
#flowdf=flowdf.groupby(['Currency','Date']).sum().reset_index()


#flowdf.set_index(['Currency','Date']).groupby(level=0,as_index=False).cumsum().reset_index()

#myCoins=['ETH','BTC','XRP','BAT','ZRX','PIVX','UBIQ']

df_dict = {}
for coin in myCoins:
    histo = h.histoDay(coin,'USD',allData=True)
    if histo['Data']:
        df_histo = pd.DataFrame(histo['Data'])
        df_histo['time'] = pd.to_datetime(df_histo['time'],unit='s')
        #df_histo=df_histo[df_histo['time']>=mysd]
        #df_histo.index = df_histo['time']
        #del df_histo['time']
        #del df_histo['volumefrom']
        #del df_histo['volumeto']
        df_dict[coin] = df_histo


df=pd.concat(df_dict).reset_index()
df=df.drop(['level_1'],axis=1)
df['mean']=(df['high']+df['low']+df['open']+df['close'])/4

#df['time']=df['time'].dt.strftime("%Y-%m-%d")


#df.insert(0,"concat",df['level_0']+'_'+df['time'])

df=df.rename(columns={"level_0":"currency"})


da=df.head()


df=pd.merge(da, flowdf, how="left", left_on=['time','currency'], right_on=['Date','Currency']).fillna(0)

df=df.sort_values(['time'])
df['cumsum']=df.groupby(['currency'])['Transfer'].cumsum()

df['USDVal']=df['mean']*df['cumsum']


df=df.pivot(index='time',columns='currency',values='USDVal')

df['TotalValue']=df[myCoins].sum(axis=1)
df['Principle']=pd.merge(df, idf, how="outer", right_index=True, left_index=True).fillna(0)['Total USD'].cumsum()

df_coins=df[myCoins].reset_index()

#df_overview=

df=df.reset_index()

#flowdf=flowdf.groupby(['Currency','Date']).sum().reset_index()

#flowdf['cumsum']=flowdf.groupby(['Currency','Date'])['Transfer'].cumsum()


#df.set_index(['currency','time']).groupby(level=0,as_index=False).cumsum().reset_index()

#df['USD Spend by c','c_cumsum']=df.groupby(['Currency'])['Total USD','Transfer'].cumsum()

#sd=datetime.datetime.strptime('2017-08-26' , '%Y-%m-%d').date()
#ed=datetime.date.today()

#delta=ed-sd

#myDays=[]
#for i in range(delta.days + 1):
#	myDays.append(sd + datetime.timedelta(days=i))

#myDays=pd.DataFrame(myDays, columns=['Date'])

#myDays['Date']=pd.to_datetime(myDays['Date'])

#myDays=pd.merge(myDays, da, how="inner", left_on=['Date'], right_on=['time'])


#pd.merge(myDays, df, how="outer", left_on=['Date','Currency'], right_on=['Date','Currency']).fillna(0)

df.to_csv("/data/daily.csv", sep=",", index=None, header=True)

df.to_csv("/home/bitnami/temp/coins.csv", sep=",", index=None, header=True)

os.system("python /home/bitnami/sheets/tosheet.py '/home/bitnami/temp/coins.csv' 'Crypto Currencies' 'Daily' 0")
