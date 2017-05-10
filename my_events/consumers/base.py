from __future__ import print_function
from kafka import KafkaConsumer
from my_events.config import KAFKA_BROKERS


class BaseConsumer(object):
    def __init__(self, topic, consumer=None):
        self.topic = topic
        self._consumer = consumer

    @property
    def client(self):
        if self._consumer is None:
            self._consumer = KafkaConsumer(
                bootstrap_servers=['localhost:9092'],#KAFKA_BROKERS,
                auto_offset_reset='earliest'
            )

            self._consumer.subscribe([self.topic])
        return self._consumer

    def get_event_details(self, message):
        raise NotImplemented

    def get_attendee_details(self, message):
        raise NotImplemented

    def run(self):
        for message in self:
            self.process_message(message.value)

    def process_message(self, message):
        parsed_message = {
            'event': self.get_event_details(message),
            'attendee': self.get_attendee_details(message)
        }

        print(parsed_message)

    def __iter__(self):
        return self.client
