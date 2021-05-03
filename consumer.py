from confluent_kafka import Consumer
from config import conf

if __name__ == "__main__":

    consumer = Consumer(conf)
    consumer.subscribe(['articles'])

    while True:
        msg = consumer.poll(1.0)

        if msg is None:
            continue
        if msg.error():
            print("Consumer error: {}".format(msg.error()))
            continue

        print("Received Messages: {}".format(msg.value().decode('utf-8')))

    consumer.close()
