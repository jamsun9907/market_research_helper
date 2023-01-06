# import config
# from pymongo import MongoClient
# # Replace the uri string with your MongoDB deployment's connection string.
# HOST = config.HOST
# USER = config.USER
# PASSWORD = config.PASSWORD
# DATABASE_NAME = 'CP1_DB'
# COLLECTION_NAME = 'population'
# MONGO_URI = f"mongodb+srv://{USER}:{PASSWORD}@{HOST}/{DATABASE_NAME}?retryWrites=true&w=majority"
#
# # 커넥션 접속 작업
# client = MongoClient(MONGO_URI)
# db = client[DATABASE_NAME] # Connection
# collection = db[COLLECTION_NAME] # Creating table
#
# # find code goes here
# cursor = collection.find({"SENSING_TIME": True})
# # iterate code goes here
# for doc in cursor:
#     print(doc)
# # Close the connection to MongoDB when you're done.
# client.close()

import config
import requests
import json

START_INDEX = 95470
END_INDEX = START_INDEX + 3
API = f'http://openapi.seoul.go.kr:8088/{config.KEY}/json/IotVdata018/{START_INDEX}/{END_INDEX}/'


# Parsing
response = requests.get(API)
status = response.status_code  # 400
text = json.loads(response.text)

if status == 200:
    print(text)  # 구하고자 하는 rows

