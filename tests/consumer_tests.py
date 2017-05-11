import unittest

from mock import Mock, patch, call

from my_events.consumers.base import BaseConsumer
from my_events.consumers import AlphaConsumer


class Message(object):
    def __init__(self, value):
        self.value = value


class BaseConsumerTestCase(unittest.TestCase):
    def setUp(self):
        kafka_patcher = patch(
            'my_events.consumers.base.KafkaConsumer'
        )

        self.kafka = kafka_patcher.start()
        self.addCleanup(kafka_patcher.stop)

    def test_client_property_does_not_call_kafka_constructor_if_set(self):
        consumer = BaseConsumer('my-topic', (i for i in xrange(10)))

        counter = 0
        for _ in consumer:
            counter += 1

        self.kafka.assert_not_called()

    def test_client_calls_kafka_constructor_if_not_set(self):
        consumer = BaseConsumer('my-topic')

        consumer.client

        self.assertEqual(self.kafka.call_count, 1)

    def test_can_iterate_over_consumer(self):
        consumer = BaseConsumer('my-topic', (str(i) for i in xrange(10)))

        counter = 0
        for _ in consumer:
            counter += 1

        self.assertEqual(counter, 10)

    def test_does_not_call_process_message_if_iterator_is_empty(self):
        consumer = BaseConsumer('my-topic', iter([]))

        process_mock = Mock()
        consumer.process_message = process_mock

        counter = 0
        for _ in consumer:
            counter += 1

        self.assertEqual(counter, 0)

    def test_calls_process_message_for_each_message_in_iterator(self):
        consumer = BaseConsumer('my-topic', iter([Message('1'), Message('2'), Message('3')]))

        process_mock = Mock()
        consumer.process_message = process_mock

        expected_calls = [call('1'), call('2'), call('3')]

        consumer.run()

        process_mock.assert_has_calls(expected_calls)


class AlphaConsumerTestCase(unittest.TestCase):
    def test_get_attendee_returns_name_without_company(self):
        consumer = AlphaConsumer('topic', iter([]))

        expected_result = {
            'name': 'My Name',
            'company': None,
            'website': 'http://google.com',
            'twitter': None
        }

        actual_result = consumer.get_attendee_details("my event,1/1/1970,My Name,http://google.com")

        self.assertEqual(expected_result, actual_result)

    def test_get_attendee_returns_name_with_company(self):
        consumer = AlphaConsumer('topic', iter([]))

        expected_result = {
            'name': 'My Name',
            'company': 'Amazon',
            'website': 'http://amazon.com',
            'twitter': None
        }

        actual_result = consumer.get_attendee_details("my event,1/1/1970,My Name from Amazon,http://amazon.com")

        self.assertEqual(expected_result, actual_result)
