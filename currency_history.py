import json
import pandas as pd
from currency import crycompare
from sqlalchemy import create_engine


def get_all_coins_list():
    #All Coins List
    coinList = p.coinList()
    myCoins = sorted(list( coinList['Data'].keys() ))
    return myCoins



def update_days(coinList, numDays):
    #MySQL Authorization Docs
    s=open('/home/bitnami/auth/mysql.auth','r').read()
    myUser=json.loads(s)['user']
    myPass=json.loads(s)['pass']
    
    #MySQL sqlalchemy engine
    engine = create_engine('mysql://'+ myUser +':' +myPass+'@127.0.0.1:3306/crypto', echo=False)
    
    #CryptoCompare History, imported from crycompare
    h = crycompare.History()
       #myCoins=['ETH','BTC','XRP','BAT','ZRX','PIVX','UBIQ']
    df_dict = {}
    for coin in myCoins:
        histo = h.histoDay(coin,'USD',limit=numDays)
        if histo['Data']:
            df_histo = pd.DataFrame(histo['Data'])
            df_histo['date'] = pd.to_datetime(df_histo['time'],unit='s')
            df_dict[coin] = df_histo
    
    #Recreate df from dictionary of DFs, seems like this could be just DFs bound in for loop?
    df=pd.concat(df_dict).reset_index()
    df=df.drop(['level_1'],axis=1)
    
    #Simple Mean of price columns, also creates a moving mean through day
    df['mean']=(df['high']+df['low']+df['open']+df['close'])/4
    
    #Renames for clarity
    df=df.rename(columns={"level_0":"currency"})
    
    #MySQL Query, based on Primary or Unique key
    query = """INSERT INTO prices_usd_day (currency, close, high, low, open, volumefrom, volumeto, mean, date) VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s) ON DUPLICATE KEY UPDATE currency=%s, date=%s"""
    
    #Insert and Update into MySQL, note that row.mean returns the pandas.Series() function mean
    for i, row in df.iterrows():
        engine.execute(query, (row.currency, row.close, row.high, row.low, row.open, row.volumefrom, row.volumeto, row['mean'], row.date, row.currency, row.date))
    
    
