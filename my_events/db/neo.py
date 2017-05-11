from datetime import datetime
from py2neo import Graph
from py2neo.ogm import GraphObject, Property, RelatedTo, RelatedFrom


__all__ = ['Neo4J']


class Event(GraphObject):
    __primarykey__ = 'eventid'

    def __init__(self, eventid, name, category, twitter, date):
        self.eventid = eventid
        self.name = name
        self.category = category
        self.twitter = twitter
        self.date = date

    eventid = Property()
    name = Property()
    category = Property()
    twitter = Property()
    date = Property()

    attendees = RelatedFrom('Attendee', "ATTENDED")


class Attendee(GraphObject):
    __primarykey__ = 'name'
    name = Property()
    company = Property()
    twitter = Property()
    website = Property()

    attended = RelatedTo(Event)


class FailedMessage(GraphObject):
    __primarykey__ = 'key'
    def __init__(self, format_name, message):
        self.key = '{format_name}-{date}'.format(
            format_name=format_name,
            date=datetime.now().strftime('%Y%m%d%H%M%S%f')
        )
        self.format_name = format_name
        self.message = message


    key = Property()
    format_name = Property()
    message = Property()


class Neo4J(object):
    def __init__(self, connection_string):
        self._connection_string = connection_string
        self._graph = None

    @property
    def db(self):
        if self._graph is None:
            self._graph = Graph(self._connection_string)

        return self._graph

    def add_attendee_to_event(self, attendee, event):
        event.attendees.add(attendee)
        self.db.push(event)

    def create_events(self, stream):
        for event in stream:
            event['date'] = self._parse_datetime(event['date'])
            self.db.create(Event(**event))

    def ensure_indexes(self):
        pass

    def get_event_by_name_and_date(self, name, date):
        return Event.select(self.db).where(
            "_.name = '{name}' and _.date = '{date}'".format(
                name=name,
                date=self._parse_datetime(date)
            )
        ).first()

    def get_event_by_twitter_username_and_month(self, twitter_username, month):
        try:
            month = int(month)
        except (TypeError, ValueError):
            return None

        return Event.select(self.db).where(
            "_.twitter = '{username}' and _.date =~ '.*/{month}/.*'".format(
                username=twitter_username,
                month=month
            )
        ).first()

    def get_event_by_twitter_username_and_date(self, twitter_username, date):
        return Event.select(self.db).where(
            "_.twitter = '{username}' and _.date = '{date}'".format(
                username=twitter_username,
                date=self._parse_datetime(date)
            )
        ).first()

    def get_or_update_attendee(self, query, fields_to_update):
        criteria = [
            "_.{key} = '{value}'".format(key=key, value=value) for key, value in query.iteritems()
        ]

        lookup = ' or '.join(criteria)
        fields_to_update.update(query)

        attendee = Attendee.select(self.db).where(lookup).first()
        if not attendee:
            attendee = Attendee()

        for key, value in fields_to_update.iteritems():
            setattr(attendee, key, value)

        self.db.push(attendee)
        return attendee

    def save_for_future_processing(self, format_name, message):
        self.db.create(FailedMessage(format_name, message))

    def _parse_datetime(self, date):
        return '{d.day}/{d.month}/{d.year}'.format(d=date)
