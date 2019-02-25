import requests
import datetime
import os
import errno
from pprint import pprint
base_dir = os.getcwd()
import json

import pandas as pd
datelist = pd.date_range('20190201', periods=24).tolist()
# print(datelist)
# print(datelist)
updated_datelist = []
for date in datelist:
    cleaned_date = date.strftime('%Y%m%d')
    updated_datelist.append(cleaned_date)

with open('SP_symbols.txt','r') as f:
    data = f.readlines()

data = [i.replace('\n','') for i in data]

base_link = 'https://api.iextrading.com/1.0/stock/'

mid_link  = '/chart/date/'

for stock in data:
    print(stock)
    for date in updated_datelist:
        link = base_link + str(stock) + mid_link + str(date)
        r = requests.get(link)
        stock_price = r.json()
        file_path = os.path.join(base_dir,'stocks',str(stock))
        # print(file_path)
        # filename = "/foo/bar/baz.txt"
        if os.path.isdir(file_path) is not True:
            # print('Not True')
        # if not os.path.exists(os.path(file_path)):
            try:
                os.makedirs(file_path)
            except OSError as exc: # Guard against race condition
                if exc.errno != errno.EEXIST:
                    raise
        json_name = stock + '_' + str(date) + '.json'
        json_path = os.path.join(file_path,json_name)
        # print(json_path)
        with open(json_path, 'w') as outfile:
            json.dump(stock_price, outfile, indent=4)
