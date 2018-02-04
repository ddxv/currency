import pandas as pd

from mysql_prices import connect_mysql

engine=connect_mysql()


#df=pd.read_csv('/data/final_combined.csv')

chunks=pd.read_csv('/data/final_combined_newchunk.csv', chunksize=100000)



#MySQL Query, based on Primary or Unique key
query = """INSERT INTO btc_price_usd_minute (time, open, close, high, low, volumefrom, volumeto, mean) VALUES ('{}', {}, {}, {}, {}, {}, {}, {}) ON DUPLICATE KEY UPDATE open={}, close={}, high={}, low={}, volumefrom={}, volumeto={}, mean={}"""

for chunk in chunks:
    #Insert and Update into MySQL, note that row.mean returns the pandas.Series() function mean
    for i, row in chunk.iterrows():
        #print(row['date'])
        engine.execute(query.format(row.Timestamp, row.Open, row.Close, row.High, row.Low, row['Volume_(BTC)'], row['Volume_(Currency)'], row.Weighted_Price, row.Open, row.Close, row.High, row.Low, row['Volume_(BTC)'], row['Volume_(Currency)'], row.Weighted_Price))




