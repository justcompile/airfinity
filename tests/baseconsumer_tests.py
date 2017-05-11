import unittest
from mock import Mock, patch, call
from my_events.consumers.base import BaseConsumer
from my_events.exceptions import EventNotFound


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

        self.neo4j = Mock()
        neo4j_patcher = patch(
            'my_events.consumers.base.Neo4J',
            return_value=self.neo4j
        )

        neo4j_patcher.start()
        self.addCleanup(neo4j_patcher.stop)

        BaseConsumer.format_name = 'test'

    def tearDown(self):
        BaseConsumer.format_name = None

    def test_raises_exception_if_format_name_property_and_constructor_param_not_set(self):
        BaseConsumer.format_name = None
        self.assertRaises(RuntimeError, BaseConsumer, 'a', 'b')

    def test_format_param_overrides_property(self):
        BaseConsumer.format_name = 'base'
        expected_result = 'override'
        actual_result = BaseConsumer('a','b', consumer_format='override').format_name

        self.assertEqual(expected_result, actual_result)

    def test_client_property_does_not_call_kafka_constructor_if_set(self):
        consumer = BaseConsumer('my-topic', (i for i in xrange(10)))

        counter = 0
        for _ in consumer:
            counter += 1

        self.kafka.assert_not_called()

    def test_client_calls_kafka_constructor_if_not_set(self):
        consumer = BaseConsumer('my-topic')

        my_client = consumer.client

        self.assertEqual(self.kafka.call_count, 1)

    def test_db_property_does_not_call_neo4j_constructor_if_set(self):
        consumer = BaseConsumer('my-topic', (i for i in xrange(10)))
        consumer._db = 'I am a neo4j Object'

        my_db = consumer.db

        self.neo4j.assert_not_called()
        self.assertEqual(my_db, 'I am a neo4j Object')

    @patch('my_events.consumers.base.Neo4J')
    def test_db_property_calls_neo4j_constructor_if_not_set(self, mock_constructor):
        consumer = BaseConsumer('my-topic')

        mock_constructor.return_value = 'I am a neo4j Object'
        my_db = consumer.db

        mock_constructor.assert_called_once()
        self.assertEqual(my_db, 'I am a neo4j Object')

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

    def test_get_event_details_raises_not_implemented_error(self):
        consumer = BaseConsumer('my-topic', iter([]))

        self.assertRaises(NotImplementedError, consumer.get_event_details, "data")

    def test_get_attendee_details_raises_not_implemented_error(self):
        consumer = BaseConsumer('my-topic', iter([]))

        self.assertRaises(NotImplementedError, consumer.get_attendee_details, "data")

    @patch('__builtin__.print')
    def test_get_event_details_raising_event_not_found_adds_message_to_db_for_future_processing(self, mock_print):
        message = Message('1,2,3,4')
        consumer = BaseConsumer('my-topic', iter([message]), consumer_format='my-test')

        consumer.get_event_details = Mock(side_effect=EventNotFound)

        consumer.run()

        self.neo4j.add_attendee_to_event.assert_not_called()
        mock_print.assert_called_with('Event not found. Adding to database for future processing')

        self.neo4j.save_for_future_processing.assert_called_with('my-test', message.value)

    def test_event_and_attendee_added_is_added_to_database(self):
        consumer = BaseConsumer('my-topic', iter([Message('1,2,3,4')]))

        consumer.get_event_details = Mock(return_value={'name': 'my event'})
        consumer.get_attendee_details = Mock(return_value={'name': 'a person'})

        expected_call_args = {
            'event': {'name': 'my event'},
            'attendee': {'name': 'a person'}
        }

        consumer.run()

        self.assertEqual(consumer.get_event_details.call_count, 1)
        self.assertEqual(consumer.get_attendee_details.call_count, 1)

        self.neo4j.add_attendee_to_event.assert_called_with(**expected_call_args)
