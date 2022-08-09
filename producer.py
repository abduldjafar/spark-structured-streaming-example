import pandas as pd
from kafka import KafkaProducer
from datetime import datetime
import time
import random
import numpy as np
import os

# pip install kafka-python

kafka_topic =  "transactions"
kafka_server =  "localhost:29092"

if __name__ == "__main__":
    print("Kafka Producer Application Started ... ")

    kafka_producer_obj = KafkaProducer(bootstrap_servers=kafka_server,
                                       value_serializer=lambda x: x.encode('utf-8'))
    
    filepath = "transactions.json"

    with open(filepath,"r") as f:
        datas = f.readlines()
    
    for message in datas:
        print("Message Type: ", type(message))
        print("Message: ", message)
        kafka_producer_obj.send(kafka_topic, message)
        time.sleep(1)

    print("Kafka Producer Application Completed. ")