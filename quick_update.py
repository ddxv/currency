from mysql_prices import update_prices_usd_minute, update_gdax_prices_minutes

less_mins=True
update_gdax_prices_minutes(less_mins)

update_prices_usd_minute(less_mins)

