import pymongo
from pymongo import ReturnDocument


class Mongo(object):
    def __init__(self, connection_string):
        self._connection_string = connection_string
        self._conn = None
        self._db = None

    @property
    def conn(self):
        if self._conn is None:
            self._conn = pymongo.MongoClient(self._connection_string)

        return self._conn

    @property
    def db(self):
        if self._db is None:
            self._db = self.conn[self._connection_string.split('/')[-1]]

        return self._db

    def add_attendee_to_event(self, attendee, event):
        self.db.events.update({
            'eventid': event['eventid']
        }, {'$addToSet': {'attendees': attendee['_id']}})

    def get_event_by_name_and_date(self, name, date):
        return self.db.events.find_one({'name': name, 'date': date}, {'_id': 0})

    def get_event_by_twitter_username_and_month(self, twitter_username, month):
        try:
            return self.db.events.find_one({
                'twitter': twitter_username,
                '$where': 'return this.date.getMonth() == {}'.format(int(month)-1)  # Javascript months are 0 based
            }, {'_id': 0})
        except (TypeError, ValueError):
            pass

    def get_event_by_twitter_username_and_date(self, twitter_username, date):
        try:
            return self.db.events.find_one({
                'twitter': twitter_username,
                'date': date
            }, {'_id': 0})
        except (TypeError, ValueError):
            pass

    def get_or_update_attendee(self, query, fields_to_update):
        return self.db.attendees.find_one_and_update(
            query,
            {'$set': fields_to_update},
            upsert=True,
            projection={'_id': True},
            return_document=ReturnDocument.AFTER
        )

    def save_for_future_processing(self, format_name, message):
        self.db.failed_messages.insert({
            'format_name': format_name,
            'message': message
        })
