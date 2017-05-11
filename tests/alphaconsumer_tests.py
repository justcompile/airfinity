import unittest
import datetime

from mock import Mock, patch

from my_events.consumers import AlphaConsumer
from my_events.exceptions import EventNotFound


class AlphaConsumerTestCase(unittest.TestCase):
    def setUp(self):

        self.mongo = Mock()

        mongo_patcher = patch(
            'my_events.consumers.base.Mongo',
            return_value=self.mongo
        )

        mongo_patcher.start()
        self.addCleanup(mongo_patcher.stop)

    def test_raises_exception_if_event_does_not_exist(self):
        consumer = AlphaConsumer('topic', iter([]))

        self.mongo.get_event_by_name_and_date.return_value = None

        self.assertRaises(
            EventNotFound,
            consumer.get_event_details,
            "my event,1/1/1970,My Name,http://google.com"
        )

        self.mongo.get_event_by_name_and_date.assert_called_with(
            'my event', datetime.datetime(1970, 1, 1)
        )

    def test_returns_event_details_if_event_exists(self):
        consumer = AlphaConsumer('topic', iter([]))

        self.mongo.get_event_by_name_and_date.return_value = {
            'name': 'My Name',
            'date': datetime.datetime(1970, 1, 1)
        }

        expected_args = [
            {'$or': [{'name': 'My Name'}, {'website': 'http://google.com'}]},
            {'website': 'http://google.com', 'name': 'My Name'}
        ]

        actual_result = consumer.get_event_details(
            "my event,1/1/1970,My Name,http://google.com"
        )

        self.mongo.get_event_by_name_and_date.assert_called_with(
            'my event',
            datetime.datetime(1970, 1, 1)
        )


    def test_get_attendee_returns_name_without_company(self):
        consumer = AlphaConsumer('topic', iter([]))

        consumer.get_attendee_details("my event,1/1/1970,My Name,http://google.com")
        expected_args = [
            {'$or': [{'name': 'My Name'}, {'website': 'http://google.com'}]},
            {'website': 'http://google.com', 'name': 'My Name'}
        ]

        self.mongo.get_or_update_attendee.assert_called_with(*expected_args)

    def test_get_attendee_returns_name_with_company(self):
        consumer = AlphaConsumer('topic', iter([]))

        consumer.get_attendee_details("my event,1/1/1970,My Name from Amazon,http://amazon.com")

        expected_args = [
            {'$or': [{'name': 'My Name'}, {'website': 'http://amazon.com'}]},
            {'website': 'http://amazon.com', 'company': 'Amazon', 'name': 'My Name'}
        ]

        self.mongo.get_or_update_attendee.assert_called_with(*expected_args)
