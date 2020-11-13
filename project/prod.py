from kafka import KafkaProducer
import logging
from json import dumps, loads
import csv

logging.basicConfig(level=logging.INFO)

producer = KafkaProducer(bootstrap_servers='127.0.0.1:9092', value_serializer=lambda K:dumps(K).encode('utf-8'))

with open('/home/phani/PycharmProjects/Cassandra/project/transactions.csv', 'r') as file:
    reader = csv.reader(file, delimiter=',')
    for messages in reader:
        producer.send('transactions', messages)
        producer.flush()

