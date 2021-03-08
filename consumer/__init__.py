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

# stream 1 consumer
def A(num, stream, group):
    logging.debug("Consumer %d: starting", num)

    try:
        r = redis.Redis(connection_pool=config.pool, socket_timeout=1, socket_connect_timeout=1, retry_on_timeout=None)
    except Exception as e:
        logging.error("Consumer %d: Exception %s", num, e)

    try:
        r.xgroup_create('STREAM_' + str(stream), 'GROUP_' + str(group), mkstream=True)
    except Exception as e:
        logging.error("Consumer %d: Exception %s", num, e)
    
    time.sleep(3)

    while True:
    
        try:
            # ci sono elementi dello stream inevasi? da altri threads?
#            pending=r.xpending('STREAM_' + str(num), 'GROUP_' + str(group))
#            if pending['pending']>0: 
#                logging.error("Thread %s: %s PENDING STREAMS", num, pending)
#                pending=r.xpending_range('STREAM_' + str(num), 'GROUP_' + str(group), '-', '+', 2)
#                for p in pending:
#                    logging.error("Thread %s: %s PENDING STREAM", num, p['message_id'])
#                    r.xclaim('STREAM_' + str(num), 'GROUP_' + str(group), 'CONSUMER_' + str(num), 100, p['message_id'])

            #data = r.execute_command('XREADGROUP GROUP GROUP_%d CONSUMER_%d BLOCK 2000 COUNT 10 STREAMS STREAM_%d >' % (group, num, stream))
            data = r.execute_command('XREADGROUP GROUP GROUP_%d CONSUMER_%d BLOCK 0 COUNT 100 STREAMS STREAM_%d >' % (group, num, stream))
            
            if not data:
                logging.debug("Consumer %d: TIMEOUT", num)
            else:
                l = r.xlen('STREAM_' + str(stream))
                logging.debug("Consumer %d: [%d] %s", num, l, data)

                c = 0
                for i in data[0][1]:
                    if i[0]:
                        
                        c = c + 1

                        id = i[0]
        
                        # consumer code
                        logging.debug("Consumer %d: Start %s", num, id)




                        logging.debug("Consumer %d: End %s", num, id)
                        # consumer code
                        
                        r.execute_command('XACK STREAM_%d GROUP_%d %s' % (stream, num, id))
                        r.execute_command('XDEL STREAM_%d %s' % (stream, id))
                        
                        tot = r.xlen('STREAM_' + str(num))
                        logging.debug("Consumer %d: XACK %s, XDEL %s [%d/%d]", num, id, id, c, tot)

            #time.sleep(1)

        except Exception as e:
            logging.error("Consumer %d: Exception %s", num, e)

    logging.error("Consumer %d: stopped!", num)
