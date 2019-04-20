from time import sleep
from kafka import KafkaProducer
import requests
import sys
import json
import os
import datetime

# offset must be >=0 and less than MAX_OFFSET
MAX_OFFSET = 390

link = "https://api.iextrading.com/1.0/stock/AAPL/chart/date/{0}"

def get_stock_details(date_val):
    complete_request = link.format(date_val)
    print(complete_request)
    r = requests.get(complete_request)
    stock_details = r.json()
    return stock_details

def get_stock_details_for_minute(stock_details, offset):
    start_time = datetime.time(9, 30)
    delta = datetime.timedelta(minutes = offset)

    required_time = (datetime.datetime.combine(datetime.date(1,1,1), start_time) + delta).time()
    print(required_time)

    required_key = str(required_time.hour).zfill(2) + ":" + str(required_time.minute).zfill(2)
    print(required_key)

    required_record = stock_details[offset]
    print(required_record)



if __name__ == "__main__":
    
    if len(sys.argv) == 2:
        date_val = sys.argv[1]
    else:
        date_val = "20190418"

    stock_details = get_stock_details(date_val)
    if stock_details and len(stock_details) > 0:
        stock_details = stock_details

        get_stock_details_for_minute(stock_details, 390)
    else:
        print("No stock details")
    # print(stock_details)






