from my_events.consumers.base import BaseConsumer
from my_events.exceptions import EventNotFound


class BetaConsumer(BaseConsumer):
    """
    Format:
    event_twitter: string, starts with @
    event_month: number
    who: string, may contain spaces
    person_twitter: string, starts with @
    """

    def get_event_details(self, message):
        event_twitter, event_month, attendee, attendee_twitter = message.split(',')

        event = self.db.get_event_by_twitter_username_and_month(event_twitter, event_month)

        if not event:
            raise EventNotFound

        return event

    def get_attendee_details(self, message):
        event_twitter, event_month, attendee, attendee_twitter = message.split(',')

        query = {
            '$or': [{'name': attendee}, {'twitter': attendee_twitter}]
        }

        return self.db.get_or_update_attendee(query, {'name': attendee, 'twitter': attendee_twitter})
