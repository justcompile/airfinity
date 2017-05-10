from dateutil import parser
from my_events.consumers.base import BaseConsumer


class GammaConsumer(BaseConsumer):
    """
    Format:
    event_twitter: string @username
    event_date: string in format d/m/yyyy
    site: string website url
    twitter: string @username
    """

    def get_event_details(self, message):
        event_twitter, date, attendee_website, attendee_twitter = message.split(',')

        return {
            'name': None,
            'date': parser.parse(date, dayfirst=True),
            'twitter': event_twitter
        }

    def get_attendee_details(self, message):
        event_twitter, date, attendee_website, attendee_twitter = message.split(',')

        parsed_attendee = {
            'name': None,
            'company': None,
            'website': attendee_website,
            'twitter': attendee_twitter
        }

        return parsed_attendee
