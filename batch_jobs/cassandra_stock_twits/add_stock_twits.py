# Python script to add stock twits to Cassandra table in the format specified
# symbol | created_at | body | entities_sentiment_basic | id | likes_total | user_followers | user_name | user_username
'''
CREATE TABLE bigdata.stock_twits (
   ...   symbol text,
   ...   created_at double,
   ...   id double,
   ...   body text,
   ...   user_followers double,
   ...   user_username text,
   ...   user_name text,
   ...   likes_total double,
   ...   entities_sentiment_basic text ,
   ...   PRIMARY KEY (symbol, created_at)
   ... );
'''

# pip install cassandra-driver


import os
import json
import datetime
from cassandra.cluster import Cluster

epoch = datetime.datetime.utcfromtimestamp(0)

def unix_time_millis(dt):
    return int((dt - epoch).total_seconds() * 1000)

cluster = Cluster(['18.136.251.110'])
session = cluster.connect('bigdata')

twits_location = "twits"
for f in os.listdir(twits_location):
    file_location = os.path.join(twits_location, f)
    file_content = None
    with open(file_location, "r") as fr:
        file_content = fr.read()

    if file_content:
        twit_details = json.loads(file_content)
        messages = twit_details["messages"]
        print("{0} messages found".format(len(messages)))
        for m in messages:
            required_details = {}
            required_details["symbol"] = f.replace(".json", "")
            required_details["created_at"] = unix_time_millis(datetime.datetime.strptime(m["created_at"], "%Y-%m-%dT%H:%M:%SZ"))
            required_details["id"] = m["id"]
            required_details["body"] = m["body"]
            required_details["user_followers"] = m["user"].get("followers") if m.get("user") else None
            required_details["user_username"] = m["user"].get("username") if m.get("user") else None
            required_details["user_name"] = m["user"].get("name") if m.get("user") else None
            required_details["likes_total"] = m["likes"].get("total") if m.get("likes") else None
            required_details["entities_sentiment_basic"] = m["entities"]["sentiment"].get("basic") if m.get("entities") and m.get("entities").get("sentiment") else None
            print(required_details)

            try:
                # insert into Cassandra
                session.execute(
                """
                INSERT INTO stock_twits (symbol, created_at, id, body, user_followers, user_username, user_name, likes_total, entities_sentiment_basic)
                VALUES (%(symbol)s, %(created_at)s, %(id)s, %(body)s, %(user_followers)s, %(user_username)s, %(user_name)s, %(likes_total)s, %(entities_sentiment_basic)s)
                """, 
                required_details
                )
                print("Inserted record...")
            except Exception as e:
                print("An error occurred while inserting: " + str(e))


