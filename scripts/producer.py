from argparse import ArgumentParser, FileType
from pyarrow.parquet import ParquetFile
from configparser import ConfigParser
from confluent_kafka import Producer
from random import choice
import pyarrow as pa
import json
import time
import sys

# Parse the command line.
parser = ArgumentParser()

# Read input file
parser.add_argument('config_file', type=FileType('r'), 
                    help='configuration file with ports, group_id, etc')

# Read name of topic
parser.add_argument('topic_name', type=str, 
                    help='name of topic (has to be the tag for parquet files in ../data/raw)')

# Read size of bath that will be read
parser.add_argument('size', type=int, 
                    help='size of batch for file in ../data/raw')

args = parser.parse_args()

# Parse the configuration.
# See https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
config_parser = ConfigParser()
config_parser.read_file(args.config_file)
config = dict(config_parser['default'])

# Create Producer instance
producer = Producer(config)
        
# Topic name (has to be the tag for parquet files in ../data/raw)
topic_name = args.topic_name

# Number of rows to be read from parquet file
size=args.size

# Sample with the first "size" rows of dataframe with tag "topic_name"
pf = ParquetFile('../data/raw/202201W1-' + topic_name + '.parquet') 
nrows = next(pf.iter_batches(batch_size = size)) 
df = pa.Table.from_batches([nrows]).to_pandas() 

# Average time between timestamps for entire dataframe
average_time = df['timestamp'].diff().mean().total_seconds() 

# Publish data
for i in range(df.shape[0]):
    message = df.iloc[i].to_dict()
    message['timestamp']=str(message['timestamp'])
    m = json.dumps(message)
    
    print("Sending: {}".format(message))
    
    producer.produce(topic=topic_name, value= m.encode('utf-8'))
    
    time.sleep(average_time)

producer.flush()
