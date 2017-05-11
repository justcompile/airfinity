from __future__ import print_function
from kafka import KafkaConsumer
from my_events.config import MONGO_CONNECTION_STRING
from my_events.db import Mongo
from my_events.exceptions import EventNotFound


class BaseConsumer(object):
    def __init__(self, topic, consumer=None):
        self.topic = topic
        self._consumer = consumer
        self._db = None

    @property
    def client(self):
        if self._consumer is None:
            self._consumer = KafkaConsumer(
                bootstrap_servers=['localhost:9092'],#KAFKA_BROKERS,
                auto_offset_reset='earliest'
            )

            self._consumer.subscribe([self.topic])
        return self._consumer

    @property
    def db(self):
        if self._db is None:
            self._db = Mongo(MONGO_CONNECTION_STRING)

        return self._db

    def get_event_details(self, message):
        raise NotImplemented

    def get_attendee_details(self, message):
        raise NotImplemented

    def run(self):
        for message in self:
            try:
                self.process_message(message.value)
            except EventNotFound:
                print('Not found')
                continue

    def process_message(self, message):
        parsed_message = {
            'event': self.get_event_details(message),
            'attendee': self.get_attendee_details(message)
        }
        #print(parsed_message)

        self.db.add_attendee_to_event(**parsed_message)

    def __iter__(self):
        return self.client
