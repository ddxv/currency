import gdax
import pandas as pd
import json


def initialize_gdax():
    global joker
    joker = False
    gdax_auth = json.load(open('/home/bitnami/auth/gdax'))
    secret=gdax_auth['secret']
    key=gdax_auth['key']
    passphrase=gdax_auth['passphrase']
    ac = gdax.AuthenticatedClient(key, secret, passphrase)
    return(ac)



#LIVE ACCOUNT
ac = initialize_gdax()

my_fills = ac.get_fills()

my_fills = pd.DataFrame(flat_list)

flat_list = [item for sublist in my_fills for item in sublist]

