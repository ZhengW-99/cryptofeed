import pymongo
import pandas as pd
from config import *

client = pymongo.MongoClient(db_uri)

# mydb = client['ftx']
mydb = client['bybit']
A = mydb.list_collection_names()
# mycol = mydb[A[1]]
for col in A:
    print(col)
    mycol = mydb[col]
    L = list(mycol.find())
    print(L[0])
#     mycol.drop()