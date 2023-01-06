import config
import pandas as pd
from pymongo import MongoClient

# Replace the uri string with your MongoDB deployment's connection string.
HOST = config.HOST
USER = config.USER
PASSWORD = config.PASSWORD
DATABASE_NAME = 'CP1_DB'
COLLECTION_NAME = 'population'
MONGO_URI = f"mongodb+srv://{USER}:{PASSWORD}@{HOST}/{DATABASE_NAME}?retryWrites=true&w=majority"

# 커넥션 접속 작업
client = MongoClient(MONGO_URI)
db = client[DATABASE_NAME] # Connection
collection = db[COLLECTION_NAME] # Creating table

# Converting cursor to the list of dictionaries
cursor = collection.find()
list_cur = list(cursor)
df = pd.DataFrame(list_cur)

# Printing the df to console
print(df.head())

# Close the connection to MongoDB when you're done.
client.close()


