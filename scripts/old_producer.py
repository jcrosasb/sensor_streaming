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
parser.add_argument('config_file', type=FileType('r'))
args = parser.parse_args()
        
# Parse the configuration.
# See https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
config_parser = ConfigParser()
config_parser.read_file(args.config_file)
config = dict(config_parser['default'])

# Create Producer instance
producer = Producer(config)
        
# Topic name (has to be the tag for parquet files in ../data/raw)
topic_name = dict(config_parser['file'])['topic_name'] 

# Number of rows to be read from parquet file
size = int(dict(config_parser['file_size'])['size'])

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
