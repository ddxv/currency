import mysql_prices
from time import sleep,time

then = time() + 40
while time() < then:
    #Update MySQL
    less_mins=True
    mysql_prices.update_prices_usd_minute(less_mins)
    sleep(2)



