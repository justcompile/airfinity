import re
from dateutil import parser
from my_events.consumers.base import BaseConsumer


class AlphaConsumer(BaseConsumer):
    """
    Format:
    eventname: Event name as string. May contain spaces
    eventdate: String in format d/m/yy.
    who: Name of Attendee. May be person, company name or "person from company"
    site: Web Address
    """

    def get_event_details(self, message):
        event_name, date, attendee, site_url = message.split(',')

        return {
            'name': event_name,
            'date': parser.parse(date, dayfirst=True),
            'twitter': None
        }

    def get_attendee_details(self, message):
        event_name, date, attendee, site_url = message.split(',')

        parsed_attendee = {
            'name': attendee,
            'company': None,
            'website': site_url,
            'twitter': None
        }

        company_name_matcher = re.search(r'^(?P<name>[\w\s]+) from (?P<company>.*)$', attendee)
        if company_name_matcher:
            parsed_attendee['name'] = company_name_matcher.group('name')
            parsed_attendee['company'] = company_name_matcher.group('company')

        return parsed_attendee
