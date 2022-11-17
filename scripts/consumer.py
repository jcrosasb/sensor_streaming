from argparse import ArgumentParser, FileType
from configparser import ConfigParser
from confluent_kafka import Consumer, OFFSET_BEGINNING
import sys
import ast

# Parse the command line.
parser = ArgumentParser()

# Read input file
parser.add_argument('config_file', type=FileType('r'))

# Read name of topic
parser.add_argument('topic_name', type=str, 
                    help='name of topic (has to be the tag for parquet files in ../data/raw)')

parser.add_argument('--reset', action='store_true')
args = parser.parse_args()

# Parse the configuration.
# See https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
config_parser = ConfigParser()
config_parser.read_file(args.config_file)
config = dict(config_parser['default'])
config.update(config_parser['consumer'])

# Create Consumer instance
consumer = Consumer(config)

# Topic name (has to be the tag for parquet files in ../data/raw)
topic_name = args.topic_name

# Consume data
consumer.subscribe([topic_name])
while True:
    msg=consumer.poll(1.0) #timeout
    if msg is None:  
        print('Waiting...')
        continue
    elif msg.error():
        print('Error: {}'.format(msg.error()))
        continue
    else:
        x = msg.value().decode('utf-8').replace('\\n',', ')
        print(x)
        x = ast.literal_eval(x)
            
consumer.close()