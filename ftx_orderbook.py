import time

import pyximport
pyximport.install(language_level=3)

from cryptofeed import FeedHandler
from cryptofeed.log import get_logger
from cryptofeed.exchanges import FTX
from cryptofeed.backends.mongo import BookMongo
from cryptofeed.defines import L2_BOOK
from config import *

def main():
    LOG = get_logger('main', LOGGING["filename"], level=LOGGING["level"])
    current_time = str(int(time.time()))
    LOG.info("Connecting to %s", db_uri)
    data = {
            L2_BOOK: BookMongo("bybit", key='bookdelta' + current_time, uri=db_uri)
            }
    
    f = FeedHandler()
    f.add_feed(
        FTX(symbols=['BTC-USD-PERP', 'ETH-USD-PERP'],
               channels=list(data.keys()),
               callbacks=data))
    f.run()


if __name__ == '__main__':
    main()
