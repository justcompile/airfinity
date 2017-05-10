from my_events.consumers.base import BaseConsumer


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

        return {
            'name': event_twitter,
            'month': event_month,
            'twitter': None
        }

    def get_attendee_details(self, message):
        event_twitter, event_month, attendee, attendee_twitter = message.split(',')

        parsed_attendee = {
            'name': attendee,
            'company': None,
            'website': None,
            'twitter': attendee_twitter
        }

        return parsed_attendee
