'''
Copyright (C) 2017-2022 Bryant Moscon - bmoscon@gmail.com

Please see the LICENSE file for the terms and conditions
associated with this software.
'''
from collections import defaultdict
from datetime import timezone, datetime as dt

import bson
import motor.motor_asyncio

from cryptofeed.backends.backend import BackendBookCallback, BackendCallback, BackendQueue

class MongoCallback(BackendQueue):
    def __init__(self, db, uri = '127.0.0.1:27017', key=None, none_to=None, numeric_type=str, **kwargs):
        self.uri = uri
        self.db = db
        self.numeric_type = numeric_type
        self.none_to = none_to
        self.collection = key if key else self.default_key
        self.running = True
        self.just = {}

    async def writer(self):
        conn = motor.motor_asyncio.AsyncIOMotorClient(self.uri)
        db = conn[self.db]
        while self.running:
            async with self.read_queue() as updates:
                for index in range(len(updates)):
                    data = updates[index]
                    await db[data['symbol'].replace("-","_") + '_' + data['_type']].update_one(data['loc'], {'$set': data}, upsert = True)
        
class TradeMongo(MongoCallback, BackendCallback):
    default_key = 'trades'
    
    async def write(self, data: dict):
        d = {'_id': data['timestamp'], 
             'receipt_timestamp': data['receipt_timestamp'], 
             'size': data['amount'], 'price': data['price'], 
             'side': True if data['side'] == "sell" else False,
             'loc': {'_id': data['timestamp']},
             'symbol': data['symbol'],
            }
        d['_type'] = 'trades'
        if 'tick_direction' in data.keys() and data['side'] != data['tick_direction']:
            d['tick_direction'] = data['tick_direction'], 
        
        if self.multiprocess:
            self.queue[1].send(d)
        else:
            await self.queue.put(d) 

class FundingMongo(MongoCallback, BackendCallback):
    default_key = 'funding'

class BookMongo(MongoCallback, BackendBookCallback):
    default_key = 'book'

    def __init__(self, *args, snapshots_only=False, snapshot_interval=1000, **kwargs):
        self.snapshots_only = snapshots_only
        self.snapshot_interval = snapshot_interval
        self.snapshot_count = defaultdict(int)
        super().__init__(*args, **kwargs)
    
    async def write(self, data: dict):
        self.gap=3600
        insert = {'_id': data['timestamp'], 
                  'receipt_timestamp': data['receipt_timestamp'],
                  'symbol': data['symbol'],
                  'exchange': data['exchange'],
                  'loc':{'_id': data['timestamp']},
                 }
        if not self.just.__contains__(data['symbol']):
            self.just[data['symbol']] = 0
        
        if (data['timestamp']//self.gap-self.just[data['symbol']]>0.5):
        # save snapshot
            insert1=insert.copy()
            price = {'ask': [], 'bid': []}
            size = {'ask': [], 'bid': []}
            name = {'ask':['ask_price', 'ask_size'], 'bid': ['bid_price', 'bid_size']}
            for side in ['bid', 'ask']:
                if len(data['book'][side]) != 0:
                    price[side]=[float(i) for i in list(data['book'][side].keys())]
                    size[side]=[0 if i is None else float(i)  for i in list(data['book'][side].values())]
                insert1[name[side][0]] = price[side] 
                insert1[name[side][1]] = size[side]
            self.just[data['symbol']]=(data['timestamp']+ 0.0001)//self.gap
            insert1['_type'] = 'book'
            if self.multiprocess:
                self.queue[1].send(insert1)
            else:
                await self.queue.put(insert1)   
                
        if data['delta'] is not None:
            price = {'ask': [], 'bid': []}
            size = {'ask': [], 'bid': []}
            name = {'ask':['ask_price', 'ask_size'], 'bid': ['bid_price', 'bid_size']}
            for side in ['bid', 'ask']:
                for i in range(0, len(data['delta'][side])):
                    price[side].append(data['delta'][side][i][0])
                    size[side].append(data['delta'][side][i][1])
                insert[name[side][0]] = price[side] 
                insert[name[side][1]] = size[side]
            insert['_type'] = 'delta'
            if self.multiprocess:
                self.queue[1].send(insert)
            else:
                await self.queue.put(insert)


class TickerMongo(MongoCallback, BackendCallback):
    default_key = 'ticker'
    
    async def write(self,data: dict):
        d = {
            '_id': data['timestamp'],
            'receipt_timestamp': data['receipt_timestamp'],
            'bid': data['bid'],  
            'ask': data['ask'],
            'loc': {'_id': data['timestamp']},
            'symbol': data['symbol'],
        }
        d['_type'] = 'ticker'
        
        if self.multiprocess:
            self.queue[1].send(d)
        else:
            await self.queue.put(d)

class OpenInterestMongo(MongoCallback, BackendCallback):
    default_key = 'open_interest'


class LiquidationsMongo(MongoCallback, BackendCallback):
    default_key = 'liquidations'


class CandlesMongo(MongoCallback, BackendCallback):
    default_key = 'candles'
