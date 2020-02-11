from kafka import KafkaConsumer
import json
import time

''' CONSUMER CLASS '''
class ConsumerServer(KafkaConsumer):    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.bootstrap_servers="localhost:9092"
        self.value_deserializer=lambda m: json.loads(m.decode('utf-8'))
        
    ''' reading messages'''
    def consume(self):
        for message in self:
            print (message.value)
        
''' consumer instance with topic subscription '''        
police_calls_kfk = ConsumerServer("udacity.project2.police.calls_3")

''' invoking reading function '''
police_calls_kfk.consume()