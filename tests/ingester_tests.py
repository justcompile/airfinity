import unittest

from mock import Mock, call

from my_events.ingester import Ingester, CSVStreamIngester


class IngesterTestCase(unittest.TestCase):
    def setUp(self):
        self.kafka_client_mock = Mock()
        
    def test_raises_exception_when_format_not_supplied(self):
        self.assertRaises(RuntimeError, Ingester, None)

    def test_raises_exception_when_no_data_is_supplied(self):
        ingester = Ingester('my-key')

        self.assertRaises(RuntimeError, ingester._route_data, None)

    def test_does_not_send_data_to_topic_if_iterable_is_empty(self):
        ingester = Ingester('my-key')
        ingester._kafka_client = self.kafka_client_mock

        ingester._route_data([])
        self.kafka_client_mock.send.assert_not_called()

    def test_sends_one_message_to_topic_if_stream_contains_one_record(self):
        ingester = Ingester('my-key')
        ingester._kafka_client = self.kafka_client_mock

        ingester._route_data(['hello'])
        self.kafka_client_mock.send.assert_called_once_with('my-key', 'hello')

    def test_sends_messages_when_stream_is_a_generator(self):
        data_stream = (i for i in xrange(10))
        expected_calls = [call('my-key', i) for i in range(10)]

        ingester = Ingester('my-key')
        ingester._kafka_client = self.kafka_client_mock

        ingester._route_data(data_stream)
        self.kafka_client_mock.send.assert_has_calls(expected_calls)

    def test_raises_not_implemented(self):
        self.assertRaises(NotImplementedError, Ingester('my-key').send_data, [])


class CSVStreamIngesterTestCase(unittest.TestCase):
    def setUp(self):
        self.ingester = CSVStreamIngester('my-key')
        self._route_mock = Mock()
        self.ingester._route_data = self._route_mock

    def test_sends_all_lines_if_has_headers_kwarg_not_supplied(self):
        data_stream = [i for i in xrange(10)]
        expected_lines = range(10)

        self.ingester.send_data(data_stream)
        self.assertEqual(expected_lines, self._route_mock.call_args[0][0])

    def test_sends_all_lines_if_has_headers_kwarg_false(self):
        data_stream = [i for i in xrange(20)]
        expected_lines = range(20)

        self.ingester.send_data(data_stream, has_headers=False)
        self.assertEqual(expected_lines, self._route_mock.call_args[0][0])

    def test_sends_all_lines_if_has_headers_kwarg_true(self):
        data_stream = [i for i in xrange(20)]
        expected_lines = range(1, 20)

        self.ingester.send_data(data_stream, has_headers=True)
        self.assertEqual(expected_lines, self._route_mock.call_args[0][0])
