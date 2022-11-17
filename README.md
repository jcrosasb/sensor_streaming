# sensor_streaming

This repository contains the files:
1. docker-compose.yml 
2. dockerfile
3. env.yml

and the following directories:
1. data/raw
2. notebooks

Before running anything, put the parquet files from sensor project (https://drive.google.com/drive/u/0/folders/1B5D3mKSEIMdRjIpap-Tg-RX2ZaI6HnrP) inside data/raw. Once this is done, do:
> docker-compose up -d

The directory "notebooks" contains the following notebooks:
1. eda_model_cart.ipynb
2. eda_model_lidar.ipynb
3. eda_model_m.ipynb
4. eda_model_speedo.ipynb



