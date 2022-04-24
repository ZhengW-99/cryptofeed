import time

import pyximport
pyximport.install(language_level=3)

from cryptofeed import FeedHandler
from cryptofeed.log import get_logger
from cryptofeed.exchanges import FTX
from cryptofeed.backends.mongo import TradeMongo, TickerMongo
from cryptofeed.defines import TRADES, TICKER
from config import *
import argparse


def main(product, log_filename):
    LOG = get_logger('main', log_filename, level=LOGGING["level"])
    current_time = str(int(time.time()))
    LOG.info("Connecting to %s", db_uri)
    data = {
            TRADES: TradeMongo("bybit", key='trades', uri=db_uri),
            TICKER: TickerMongo("bybit", key='ticker', uri=db_uri)
            }

    f = FeedHandler()
    f.add_feed(
        FTX(symbols=[product],
               channels=list(data.keys()),
               callbacks=data))
    f.run()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="")
    parser.add_argument('-p','--product', default="BTC-USDT")
    parser.add_argument('-l','--log_filename', default="feedhandler_BTC-USDT.log")
    args = parser.parse_args()
    main(args.product, args.log_filename)
