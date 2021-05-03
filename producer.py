import json
from confluent_kafka import Producer
from config import conf
from datetime import datetime, timedelta
from newsapi import NewsApiClient

# Init news api access
newsapi = NewsApiClient(api_key='570256fa98ba4955b0a3b160fa5dd99f')

# date variables
from_date = datetime.now() - timedelta(hours=48)  # Subtract 1 day from To Date
to_date = datetime.now()  # to Date
print('Articles are pulled between', from_date, 'and', to_date)

# Request for retrieving articles
all_articles = newsapi.get_everything(q='apple',  # topic
                                      from_param=from_date,  # start of interval
                                      to=to_date,
                                      language='en')  # language

print('Retrieved Articles between', from_date, 'and', to_date, '=', all_articles['totalResults'])


def delivery_report(err, msg):
    if err is not None:
        print('Message delivery failed:{}'.format(err))

    else:
        print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))


if __name__ == "__main__":

    # Create producer
    producer = Producer(conf)

    for i in all_articles["articles"]:
        print(i)
        producer.produce("articles", json.dumps(i, indent=4), callback=delivery_report)

    producer.flush()
