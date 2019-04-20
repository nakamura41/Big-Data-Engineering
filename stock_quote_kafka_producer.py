from time import sleep
from kafka import KafkaProducer
import requests
import sys
import json

link = "https://api.iextrading.com/1.0/stock/AAPL/chart/date/{0}"


if __name__ == "__main__":
    
    if len(sys.argv) == 2:
        date_val = sys.argv[1]
    else:
        date_val = "20190418"

    complete_request = link.format(date_val)
    print(complete_request)
    r = requests.get(complete_request)
    stock_details = r.json()
    # print(stock_details)
