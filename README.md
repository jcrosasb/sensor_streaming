# sensor_streaming

This repository contains the files:
1. docker-compose.yml 
2. dockerfile
3. env.yml

and the following directories:
1. data/raw
2. notebooks
3. scripts

Before running anything, put the parquet files from sensor project (https://drive.google.com/drive/u/0/folders/1B5D3mKSEIMdRjIpap-Tg-RX2ZaI6HnrP) inside directory "data/raw".

The directory "notebooks" contains the following notebooks:
1. eda_model_cart.ipynb
2. eda_model_lidar.ipynb
3. eda_model_m.ipynb
4. eda_model_speedo.ipynb

To run the containers, please do:
> docker-compose up -d --build

After this, you may open the browser and type
> localhost:8888/lab?token=intekglobal123

to open Jupyterlab, and
> localhost:9000

to open Kafdrop

You may run the notebooks and the scripts from Jupyterlab

# Exercise 1

Each of these notebooks has the solutions for Exercise 1 found in https://docs.google.com/document/d/1xdPnT4kceF9VKYDTvORRGx-TC3JAJAi0/edit

For Exercise 2, the directory "scripts" has the following files:
1. producer.py
2. consumer.py
3. input.ini 

# Exercise 2

## Instructions for running scripts
To run any of the scripts, you firt need to run:
> conda install -c conda-forge python-confluent-kafka --yes

from the command line to install Confluent Kafka.

To run `producer.py`, you will need to give the following arguments:
1. File `input.ini`, which contains information such as the port for the bootstrap servers and the group ID.
2. The name of the topic, which has to be the tag for one of the parquet files (lidarOut, lidarIn, S1, S2, cart1Floor, cart1Top, cart2, cart4x, speedoA, speedoB, speedoC, or m1, m2, ..., m7).
3. The size of the batch from the parquet file to be read.

For example, suppose you want to send the first 100 lines of the file `202201W1-lidarOut.parquet`. Then you have to type the following from the command line:
> python3 producer.py input.ini lidarOut 100

To run `consumer.py`, you also need to provide the `init.ini` file and the topic name, but not the batch size. Following the example above, you would nee to type the following:
> python3 consumer.py input.ini lidarOut


