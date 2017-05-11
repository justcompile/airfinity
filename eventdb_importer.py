import sys
from csv import DictReader
import pymongo
from dateutil import parser
from my_events.config import MONGO_CONNECTION_STRING


class EventDBImporter(object):
    def __init__(self):
        self._mongo_connection = None
        self._db = None

    @property
    def mongo_connection(self):
        if self._mongo_connection is None:
            self._mongo_connection = pymongo.MongoClient(MONGO_CONNECTION_STRING)

        return self._mongo_connection

    @property
    def db(self):
        if self._db is None:
            self._db = self.mongo_connection[MONGO_CONNECTION_STRING.split('/')[-1]]

        return self._db

    def import_from_csv(self, csv_stream):
        reader = DictReader(csv_stream)

        parsed_events = []
        for event in reader:
            event['date'] = parser.parse(event['date'], dayfirst=True)
            parsed_events.append(event)

        self.db.events.insert_many(parsed_events)


def error(msg):
    sys.stderr.write(msg)
    sys.exit(1)


if __name__ == '__main__':
    if sys.stdin.isatty():
        error('Error: Pipe the data into this application: python {0} file.csv\n'.format(sys.argv[0]))

    EventDBImporter().import_from_csv(sys.stdin)