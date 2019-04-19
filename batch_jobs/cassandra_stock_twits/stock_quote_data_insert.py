# Python script to add stock quotes to Cassandra table in the format specified
'''
CREATE TABLE bigdata.stock_quote_batch (
      symbol text,
      created_at double,
      id double,
      companyName text,
      sector text,
      date text,
      minute text,
      label text,
      high double,
      low double,
      average double,
      volume double,
      notional double,
      numberOfTrades double,
      marketHigh double,
      marketLow double,
      marketAverage double,
      marketVolume double,
      marketNotional double,
      marketNumberOfTrades double,
      open double,
      close double,
      marketOpen double,
      marketClose double,
      changeOverTime double,
      marketChangeOverTime double,
      PRIMARY KEY (symbol, created_at)
      );
'''

# pip install cassandra-driver


import os
import json
import datetime
import uuid
from cassandra.cluster import Cluster

epoch = datetime.datetime.utcfromtimestamp(0)

df = pd.read_csv('Stock_Ticker_with_GICS - Sheet.csv')

def csv_lookup(df, ticker):
    company_name = df["Company Name"][(df["Company Ticker"] == ticker)]
    sector = df["Sector (GICS®)"][(df["Company Ticker"] == ticker)]
    return company_name.get_values()[0],sector.get_values()[0]

def unix_time_millis(dt):
    return int((dt - epoch).total_seconds() * 1000)

def convert_date_to_epoch(date, minute):
    time_tuple = time.strptime(date+" "+minute, '%Y%m%d %H:%M')
    time_epoch = time.mktime(time_tuple)
    return time_epoch

cluster = Cluster(['18.136.251.110'])
session = cluster.connect('bigdata')

location = "./data/"

for root, dirs, files in os.walk(location, topdown=False):
    for name in dirs:
        for f in os.listdir(os.path.join(root, name)):
            file = os.path.join(root, name+"/"+f)
            print(file)
            with open(file, "r") as fr:
                file_content = fr.read()
            if file_content:
                stock_details = json.loads(file_content)
                for detail in stock_details:
                    stock_quote = {}
                    stock_quote["symbol"] = name
                    stock_quote["created_at"] = convert_date_to_epoch(detail["date"],detail["minute"])
                    stock_quote["id"] = None
                    company_name, sector = csv_lookup(df, name)
                    stock_quote["companyName"] = company_name
                    stock_quote["sector"] = sector
                    stock_quote["date"] = detail["date"]
                    stock_quote["minute"] = detail["minute"]
                    stock_quote["label"] = detail["label"]
                    stock_quote["high"] = detail["high"]
                    stock_quote["low"] = detail["low"]
                    stock_quote["average"] = detail["average"]
                    stock_quote["volume"] = detail["volume"]
                    stock_quote["notional"] = detail["notional"]
                    stock_quote["numberOfTrades"] = detail["numberOfTrades"]
                    stock_quote["marketHigh"] = detail["marketHigh"]
                    stock_quote["marketLow"] = detail["marketLow"]
                    stock_quote["marketAverage"] = detail["marketAverage"]
                    stock_quote["marketVolume"] = detail["marketVolume"]
                    stock_quote["marketNotional"] = detail["marketNotional"]
                    stock_quote["marketNumberOfTrades"] = detail["marketNumberOfTrades"]
                    stock_quote["open"] = detail["open"]
                    stock_quote["close"] = detail["close"]
                    stock_quote["marketOpen"] = detail["marketOpen"]
                    stock_quote["marketClose"] = detail["marketClose"]
                    stock_quote["changeOverTime"] = detail["changeOverTime"]
                    stock_quote["marketChangeOverTime"] = detail["marketChangeOverTime"]
                    #print(stock_quote)
                    
                    try:
                        
                        # insert into Cassandra
                        session.execute(
                    """
                        INSERT INTO bigdata.stock_quote_batch (symbol,created_at,id,companyName,sector,date
                        ,minute,label,high,low,average,volume,notional,numberOfTrades,marketHigh,marketLow
                        ,marketAverage,marketVolume,marketNotional,marketNumberOfTrades
                        ,open,close,marketOpen,marketClose,changeOverTime,marketChangeOverTime)
                        VALUES 
                        (%(symbol)s,%(created_at)s,%(id)s,%(companyName)s,%(sector)s,%(date)s,%(minute)s
                        ,%(label)s,%(high)s,%(low)s,%(average)s,%(volume)s,%(notional)s,%(numberOfTrades)s
                        ,%(marketHigh)s,%(marketLow)s,%(marketAverage)s,%(marketVolume)s
                        ,%(marketNotional)s,%(marketNumberOfTrades)s
                        ,%(open)s,%(close)s,%(marketOpen)s,%(marketClose)s,%(changeOverTime)s
                        ,%(marketChangeOverTime)s)
                        """, 
                        stock_quote
                        )
                        print("Inserted record...")
                    except Exception as e:
                        print("An error occurred while inserting: " + str(e))

