"""
Consumers handle the parsing and saving of incoming event data from a Kafka Topic.

Each format requires it's own class which at the very least must:

* Be a subclass of `my_events.consumers.base.BaseConsumer`
* Implement `BaseConsumer.get_event_details`
* Implement `BaseConsumer.get_attendee_details`
"""

from __future__ import print_function
from kafka import KafkaConsumer
from my_events.config import MONGO_CONNECTION_STRING, KAFKA_BROKERS
from my_events.db import Mongo
from my_events.exceptions import EventNotFound


class BaseConsumer(object):
    """Base class for Event Format Consumers"""
    def __init__(self, topic, consumer=None):
        self.topic = topic
        self._consumer = consumer
        self._db = None

    @property
    def client(self):
        if self._consumer is None:
            self._consumer = KafkaConsumer(
                bootstrap_servers=KAFKA_BROKERS,
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
        raise NotImplementedError

    def get_attendee_details(self, message):
        raise NotImplementedError

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

        print(parsed_message)

        self.db.add_attendee_to_event(**parsed_message)

    def __iter__(self):
        return self.client
