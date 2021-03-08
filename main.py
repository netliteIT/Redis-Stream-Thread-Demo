import logging
import threading
import time
import signal
import sys
import ctypes, os

import config
import producer
import consumer

# https://blog.logrocket.com/why-are-we-getting-streams-in-redis-8c36498aaac5/

release='2021.3'

def Exit_gracefully(signal, frame):
    logging.error("Signal: %s", signal)
    if isinstance(threads, list):
        for index, thread in enumerate(threads):
            logging.debug("Closing: %d %s.", index, thread)
    sys.exit(0)

if __name__ == "__main__":

    # Register the signal handlers
    signal.signal(signal.SIGTERM, Exit_gracefully)
    signal.signal(signal.SIGINT, Exit_gracefully)
    
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s.%(msecs)03d %(levelname)s:\t%(message)s', datefmt='%Y-%m-%d %H:%M:%S')

    logging.debug("Main: start")

    threads = list()
    
    # Producer threads start
    t = threading.Thread(target=producer.A, args=(1,), daemon=True)
    threads.append(t)
    t.start() 
    
#    # Producer threads start
#    t = threading.Thread(target=producer.B, args=(2,), daemon=True)
#    threads.append(t)
#    t.start() 
#
#    # Producer threads start
#    t = threading.Thread(target=producer.C, args=(3,), daemon=True)
#    threads.append(t)
#    t.start() 

    
    # Consumer threads start
    t = threading.Thread(target=consumer.A, args=(1,1,1), daemon=True)
    threads.append(t)
    t.start() 
    
    while True:

        s=[] 
        if isinstance(threads, list):
            for thread in threading.enumerate(): 
                s.append(thread.name + '[' + str(thread.ident) + ']')

        try:
            logging.debug("Main: heartbeat  Threads: [%s]", str(s)[1:-1])
        except Exception as e:
            logging.error("Main: Exception %s", e)
            
        time.sleep(60)