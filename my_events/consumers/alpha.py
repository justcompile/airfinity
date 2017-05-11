import re
from dateutil import parser
from my_events.consumers.base import BaseConsumer
from my_events.exceptions import EventNotFound


class AlphaConsumer(BaseConsumer):
    """ Processes data in of "alpha" format.

    Current implementation does not create Event records which do not already exist, but stores it for future processing

    Expected CSV Format: eventname,eventdate,who,site

    eventname: Event name as string. May contain spaces
    eventdate: String in format d/m/yy.
    who: Name of Attendee. May be person, company name or "person from company"
    site: Web Address
    """
    format_name = 'alpha'

    def get_event_details(self, message):
        event_name, date, attendee, site_url = message.split(',')
        event_record = self.db.get_event_by_name_and_date(
            event_name, parser.parse(date, dayfirst=True)
        )

        # TODO: Create new event if none found
        if not event_record:
            raise EventNotFound

        return event_record

    def get_attendee_details(self, message):
        event_name, date, attendee, site_url = message.split(',')

        parsed_attendee = {
            'name': attendee,
            'company': None,
            'website': site_url,
            'twitter': None
        }

        company_name_matcher = re.search(r'^(?P<name>[\w\s]+) from (?P<company>.*)$', attendee)

        update_fields = {'website': parsed_attendee['website']}

        if company_name_matcher:
            parsed_attendee['name'] = company_name_matcher.group('name')
            parsed_attendee['company'] = company_name_matcher.group('company')

            update_fields['company'] = parsed_attendee['company']

        update_fields['name'] = parsed_attendee['name']

        return self.db.get_or_update_attendee(
            {'$or': [{'name': parsed_attendee['name']}, {'website': parsed_attendee['website']}]},
            update_fields
        )
