import pandas as pd
from kafka import KafkaProducer
from datetime import datetime
import time
import random
import numpy as np
import os

# pip install kafka-python

KAFKA_TOPIC_NAME_CONS = os.environ.get('KAFKA_TOPIC') if os.environ.get('KAFKA_TOPIC') is not None else "ds_salaries_2"
KAFKA_BOOTSTRAP_SERVERS_CONS =  os.getenv('KAFKA_HOST')  if os.getenv('KAFKA_HOST') is not None else "localhost:29092"

if __name__ == "__main__":
    print("Kafka Producer Application Started ... ")

    kafka_producer_obj = KafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS_CONS,
                                       value_serializer=lambda x: x.encode('utf-8'))
    
    filepath = "ds_salaries.csv"
    
    flower_df = pd.read_csv(filepath)
    
    flower_list = flower_df.to_dict(orient="records")
    print(flower_list)
       

    message_list = []
    message = None
    for message in flower_list:
        
        message_fields_value_list = []
        columns_name = "id,work_year,experience_level,employment_type,job_title,salary,salary_currency,salary_in_usd,employee_residence,remote_ratio,company_location,company_size"
        
        for column_name in columns_name.split(","):
            message_fields_value_list.append(message[column_name])
        

        message = ','.join(str(v) for v in message_fields_value_list)
        print("Message Type: ", type(message))
        print("Message: ", message)
        kafka_producer_obj.send(KAFKA_TOPIC_NAME_CONS, message)
        time.sleep(1)


    print("Kafka Producer Application Completed. ")