# News-API

News-API is a python project using news-api.org and Kafka, MongoDB, Mongo DB Charts for Visualization the news for a specific topic

## Installation
- clone project on device or VM
- install required libs for news-api, kafka
- create api key for news-api.org and set in producer.py
- set up Kafka Cluster and edit the config for connecting the producer and consumer
- set up Mongo DB Atlas and Collection
- set up MongoDbAtlasSinkConnector and connect to Mongo DB Atlas Collection
- set up cron for running producer and consumer in desired interval

## Usage
- run the producer and consumer by cron every hour on VM or 
