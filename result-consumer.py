from kafka import KafkaConsumer
from json import loads


consumer = KafkaConsumer(
    'ranking_functions',
     bootstrap_servers=['localhost:29092'],
     auto_offset_reset='earliest',
     enable_auto_commit=True,
     group_id='my-group'
)

for message in consumer:
    print(message.key,message.value)