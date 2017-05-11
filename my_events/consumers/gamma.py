from dateutil import parser
from my_events.consumers.base import BaseConsumer
from my_events.exceptions import EventNotFound


class GammaConsumer(BaseConsumer):
    """ Processes data in of "gamma" format.

    Current implementation does not create Event records which do not already exist, but stores it for future processing

    Format:

    event_twitter: Twitter handle for Event as string prefixed with @
    event_date: String in format d/m/yy.
    site: Website of attendee
    twitter: Twitter handle for attendee as string prefixed with @
    """

    format_name = 'gamma'

    def get_event_details(self, message):
        event_twitter, date, attendee_website, attendee_twitter = message.split(',')

        event = self.db.get_event_by_twitter_username_and_date(event_twitter, parser.parse(date, dayfirst=True))

        if not event:
            raise EventNotFound

        return event

    def get_attendee_details(self, message):
        event_twitter, date, attendee_website, attendee_twitter = message.split(',')

        query = {
            '$or': [{'website': attendee_website}, {'twitter': attendee_twitter}]
        }

        return self.db.get_or_update_attendee(query, {'website': attendee_website, 'twitter': attendee_twitter})
