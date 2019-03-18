import requests
import datetime
import os
import errno
from pprint import pprint
base_dir = os.getcwd()
import json


with open('stock_twits_symbols.txt','r') as f:
    data = f.readlines()

data = [i.replace('\n','') for i in data]

base_link = 'https://api.stocktwits.com/api/2/streams/symbol/{0}.json'



for stock in data:
    print(stock)
    link = base_link.format(stock)
    
    r = requests.get(link)
    stock_twits = r.json()
    file_path = os.path.join(base_dir,'twits',str(stock) + ".json")

    with open(file_path, 'w') as outfile:
        json.dump(stock_twits, outfile, indent=4)
