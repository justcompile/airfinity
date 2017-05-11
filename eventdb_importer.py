from __future__ import print_function
import sys
from csv import DictReader
from dateutil import parser
from my_events.config import NEO4J_CONNECTION
from my_events.db import Neo4J


class EventDBImporter(object):
    def __init__(self, database):
        self.db = database

    def import_from_csv(self, csv_stream):
        self.db.ensure_indexes()
        reader = DictReader(csv_stream)

        parsed_events = []
        for event in reader:
            event['date'] = parser.parse(event['date'], dayfirst=True)
            parsed_events.append(event)

        self.db.create_events(parsed_events)


if __name__ == '__main__':
    if sys.stdin.isatty():
        sys.stderr.write('Error: Pipe the data into this application: python {0} file.csv\n'.format(sys.argv[0]))
        sys.exit(1)

    db = Neo4J(NEO4J_CONNECTION)
    EventDBImporter(db).import_from_csv(sys.stdin)
