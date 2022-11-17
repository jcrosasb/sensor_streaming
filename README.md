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

Each of these notebooks has the solutions for Exercise 1 found in https://docs.google.com/document/d/1xdPnT4kceF9VKYDTvORRGx-TC3JAJAi0/edit

For Exercise 2, the directory "scripts" has the following files:
1. consumer.py
2. producer.py
3. input.ini

To run it, please do:
> docker-compose up -d

After this, you may open the browser and type
> localhost:8888/lab?token=intekglobal123
to open Jupyterlab, and
> localhot:9000
to open Kafdrop



