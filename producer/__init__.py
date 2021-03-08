import random
import logging
import threading
import time
import signal
import sys
import redis
import ctypes, os

import pytz
from datetime import datetime
import json

import config

# stream 1 producer
def A(num):
    logging.debug("Producer %d: starting", num)
        
    try:
        r = redis.Redis(connection_pool=config.pool, socket_timeout=1, socket_connect_timeout=1, retry_on_timeout=None)
    except Exception as e:
        logging.error("Consumer %d: Exception %s", num, e)
    
    while True:
        now = datetime.now()
        try:
            id = r.xadd('STREAM_' + str(num), dict( [('A', now.strftime("%d/%m/%Y %H:%M:%S"))] ))
        except Exception as e:
            logging.error("Consumer %d: Exception %s", num, e)
        
        tot = r.xlen('STREAM_' + str(num))
        logging.debug("Producer %d: added %s stream element (tot %d)", num, id, tot)
    
        time.sleep(0.03)


# stream 2 producer
def B(num):
    logging.debug("Producer %d: starting", num)
        
    try:
        r = redis.Redis(connection_pool=config.pool, socket_timeout=1, socket_connect_timeout=1, retry_on_timeout=None)
    except Exception as e:
        logging.error("Consumer %d: Exception %s", num, e)
    
    while True:
        now = datetime.now()
        try:
            id = r.xadd('STREAM_' + str(num), dict( [('B', now.strftime("%d/%m/%Y %H:%M:%S"))] ))
        except Exception as e:
            logging.error("Consumer %d: Exception %s", num, e)
        
        tot = r.xlen('STREAM_' + str(num))
        logging.debug("Producer %d: added %s stream element (tot %d)", num, id, tot)
    
        time.sleep(2)

# stream 3 producer
def C(num):
    logging.debug("Producer %d: starting", num)
    
    try:
        r = redis.Redis(connection_pool=config.pool, socket_timeout=1, socket_connect_timeout=1, retry_on_timeout=None)
    except Exception as e:
        logging.error("Consumer %d: Exception %s", num, e)
        
    while True:
        
        now = datetime.now()
        try:
            id = r.xadd('STREAM_' + str(num), dict( [('B', now.strftime("%d/%m/%Y %H:%M:%S"))] ))
        except Exception as e:
            logging.error("Consumer %d: Exception %s", num, e)
        
        tot = r.xlen('STREAM_' + str(num))
        logging.debug("Producer %d: added %s stream element (tot %d)", num, id, tot)
    
        time.sleep(5)
