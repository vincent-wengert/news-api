import json
import logging
from confluent_kafka import Producer
from config import conf
from datetime import datetime, timedelta
from newsapi import NewsApiClient

# Init Logging
logging.basicConfig(filename='producer.log', filemode='w', format='%(name)s - %(levelname)s - %(message)s')


# Init news api access
newsapi = NewsApiClient(api_key='570256fa98ba4955b0a3b160fa5dd99f')

# date variables
from_date = datetime.now() - timedelta(hours=4)  # Subtract 1 day from To Date..1 Week
to_date = datetime.now()  # to Date
print('Articles are pulled between', from_date, 'and', to_date)
logging.info('Articles are pulled between', from_date, 'and', to_date)

# Keys to remove from result
keys_to_remove = ["urlToImage", "url", "description", "content"]

# Request for retrieving articles
all_articles = newsapi.get_everything(q='biden',  # topic
                                      from_param=from_date,  # start of interval
                                      to=to_date,
                                      language='en',
                                      page_size=100)  # language

print('Retrieved Articles between', from_date, 'and', to_date, '=', all_articles['totalResults'])
logging.info('Retrieved Articles between', from_date, 'and', to_date, '=', all_articles['totalResults'])

# Remove irrelevant keys from result
for i in all_articles["articles"]:
    for key in keys_to_remove:
                del i[key]


def delivery_report(err, msg):
    if err is not None:
        print('Message delivery failed:{}'.format(err))
        logging.error('Message delivery failed:{}'.format(err))

    else:
        print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))
        logging.info('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))


if __name__ == "__main__":

    # Create producer
    producer = Producer(conf)
    for i in all_articles["articles"]:
        logging.info(i)
        producer.produce("articles", json.dumps(i, indent=4), callback=delivery_report)

    producer.flush()
