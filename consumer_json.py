#!/usr/bin/python2

# Need to add comments. Will do on the final version :)
# This script listen for messages on the pippo topic and write to tmp file only

import sys 

TOPIC=sys.argv[1]
IP = sys.argv[2]
PORT = sys.argv[3]

from kafka import KafkaConsumer, codec
codec.has_snappy()

import logging
import logging.handlers as handlers

from pygments import highlight, lexers, formatters

import json

logger = logging.getLogger('vt')
logger.setLevel(logging.INFO)

LOG_FILE="/tmp/consumer_"+TOPIC+".log"
open(LOG_FILE, 'w').close()

logHandler = handlers.RotatingFileHandler(LOG_FILE, maxBytes=900000, backupCount=2)
logHandler.setLevel(logging.INFO)
logger.addHandler(logHandler)

import datetime
from pytz import reference

# Create a new Kafka consumer and ask for the latest set of data being stored in the Kafka bus. # Note that you can use auto_offset_reset='earliest' for ask for all the available data.
 
#consumer = KafkaConsumer(group_id='simple',bootstrap_servers='198.18.134.26:29092', auto_offset_reset='latest')
consumer = KafkaConsumer(group_id='simple',bootstrap_servers=IP + ':' + PORT + ' ', auto_offset_reset='latest')

# Suscribe to the CDG topic

consumer.subscribe([TOPIC])

now = datetime.datetime.now()
localtime = reference.LocalTimezone()
localtime.tzname(now)
ora = now.strftime("%Y-%m-%d %H:%M ")+localtime.tzname(now)
logger.info(ora+"\nStart -> Subribed to topic " +TOPIC+"\n")

def pretty_print(payload):
    parsed = json.loads(payload)
    formatted = json.dumps(parsed, indent=2, sort_keys=True)

    logger.info("\n******************************* CDG Message *******************************\n")
    logger.info(formatted)

for message in consumer:
   cdg_message=message.value
   pretty_print(cdg_message)

   now = datetime.datetime.now()
   ora = now.strftime("%Y-%m-%d %H:%M ")+localtime.tzname(now)
   logger.info("\nEnd process -> "+ora)

# END
