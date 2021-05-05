import json
import logging
from confluent_kafka import Producer
from config import conf
from datetime import datetime, timedelta
from newsapi import NewsApiClient

# Init Logging
logging.basicConfig(filename='producer.log')

# Init news api access
newsapi = NewsApiClient(api_key='570256fa98ba4955b0a3b160fa5dd99f')

# Date and time variables
timedelta_requests = 1  # request span
from_date = datetime.now() - timedelta(hours=timedelta_requests)  #
to_date = datetime.now()  # to Date
hours_past = 100  # hours to go in past to retrieve past data
retrieve_past_articles = 0  # 1 = true, 0 = false

# Keys to remove from result
keys_to_remove = ["urlToImage", "url", "description", "content"]


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

    if retrieve_past_articles == 1:
        # Request for retrieving past articles
        for i in range(0, hours_past):
            all_articles = newsapi.get_everything(q='biden',  # topic
                                                  from_param=from_date,  # start of interval
                                                  to=to_date,
                                                  language='en',
                                                  page_size=100)  # language
            from_date = from_date - timedelta(hours=1)
            to_date = to_date - timedelta(hours=1)
            print('Retrieved Articles between', from_date, 'and', to_date, '=', all_articles['totalResults'])

            # Remove irrelevant keys from result
            for i in all_articles["articles"]:
                for key in keys_to_remove:
                    del i[key]

            for i in all_articles["articles"]:
                print(i)
                logging.info('date=%s', i)
                producer.produce("articles", json.dumps(i, indent=4), callback=delivery_report)

    else:
        all_articles = newsapi.get_everything(q='biden',  # topic
                                              from_param=from_date,  # start of interval
                                              to=to_date,
                                              language='en',
                                              page_size=100)  # language
        print('Retrieved Articles between', from_date, 'and', to_date, '=', all_articles['totalResults'])

        # Remove irrelevant keys from result
        for i in all_articles["articles"]:
            for key in keys_to_remove:
                del i[key]

        for i in all_articles["articles"]:
            print(i)
            logging.info('date=%s', i)
            producer.produce("articles", json.dumps(i, indent=4), callback=delivery_report)

        logging.info('date=%s',
                     ('Retrieved Articles between', from_date, 'and', to_date, '=', all_articles['totalResults']))

    producer.flush()
