import gdax
import mysql_prices as ms
from time import sleep,time
import pandas as pd
from datetime import datetime, timedelta
import dateutil
from sqlalchemy import text

#Get DF of all prices for list of coins, startDate is a String
def get_gdax_all():
    engine=ms.connect_mysql()
    query = text('SELECT * FROM btc_price_usd_gdax_minute')
    df=pd.read_sql(query, con=engine)
    return df

def upsert_gdax_minute(df):
    query = """INSERT INTO btc_price_usd_gdax_minute (time, open,
    close, high, low, volume, mean)
    VALUES ('{}', {}, {}, {}, {}, {}, {})
    ON DUPLICATE KEY UPDATE open={}, close={},
    high={}, low={}, volume={}, mean={}
    """
    for i, row in df.iterrows():
        #Insert and Update into MySQL, note that row.mean returns the pandas.Series() function mean
        engine.execute(query.format(row.time, row.open, row.close, row.high, row.low, row.volume, row['mean'], row.open, row.close, row.high, row.low, row.volume, row['mean']))
    result.close()


engine = ms.connect_mysql()

result = engine.execute("SELECT max(time) from btc_price_usd_gdax_minute;")
lastrun = result.fetchone()[0]
result.close()

col_names = ['time', 'low', 'high', 'open', 'close', 'volume']
pc = gdax.PublicClient()

end_time = datetime.now().replace(second = 0, microsecond = 0)
end_time = str(end_time.isoformat())
start_time = end_time


while dateutil.parser.parse(end_time) > lastrun:
    start_time = dateutil.parser.parse(start_time)
    end_time = start_time
    start_time = end_time - timedelta(minutes=200)
    start_time = str(start_time.isoformat())
    end_time = str(end_time.isoformat())
    r = pc.get_product_historic_rates('BTC-USD', start = start_time, end=end_time, granularity=60)
    if len(r) == 0:
        wait_for_it()
        r = pc.get_product_historic_rates('BTC-USD', start = start_time, end=end_time, granularity=60)
    df = pd.DataFrame(r)
    df[0] = pd.to_datetime(df[0], origin='unix', unit='s')
    df.columns = col_names
    #Simple Mean of price columns, also creates a moving mean through day
    df['mean']=(df['high']+df['low']+df['open']+df['close'])/4
    df['mean']=df['mean'].round(decimals=8)
    upsert_gdax(df)
    print("ADDED " + start_time + " THROUGH " + end_time)
    sleep(10)

