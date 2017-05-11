import os
from terminaltables import AsciiTable
from my_events.config import MONGO_CONNECTION_STRING
from my_events.csv_writer import UnicodeWriter
from my_events.db import Mongo


class ReportBuilder(object):
    def __init__(self, report_dir='reports'):
        self.mongo = Mongo(MONGO_CONNECTION_STRING)
        self.report_dir = report_dir

    def summary(self):
        table_data = [["Event", "Date", "Attendees"]]

        table_data.extend(
            [
                [event['name'], event['date'].strftime('%d %b %Y'), len(event.get('attendees', []))]
                for event in self.mongo.db.events.find()
            ]
        )

        table = AsciiTable(table_data)
        print table.table

    def attendee_breakdown(self):
        for attendee in self.mongo.db.attendees.find():
            table_data = [[attendee['name'], "Event", "Category"]]
            table_data.extend([
                ['', event['name'], event['category']]
                for event in self.mongo.db.events.find({'attendees': attendee['_id']})
            ])

            table = AsciiTable(table_data)
            print table.table
            print '\n'
    # We also want to have a view on every person, what events they attended and what event categories.

    def event_tables(self):
        for event in self.mongo.db.events.find():
            print '{name}: {date:%d %b %Y}'.format(**event)
            try:
                table_data = [["Name", "Twitter", "Website", "Company"]]

                table_data.extend([
                    [attendee.get('name', ''), attendee.get('twitter', ''), attendee.get('website', ''), attendee.get('company', '')]
                    for attendee in self.mongo.db.attendees.find({'_id': {'$in': event['attendees']}})
                ])

                table = AsciiTable(table_data)
                print table.table
            except KeyError:
                print 'No attendees'
            print '\n'


            self.write_to_file(table_data, '{name}-{date}.csv'.format(
                name=event['name'].replace(' ', '_'),
                date=event['date'].strftime('%Y%m%d')
            ))

    def print_header(self, title):
        chars_in_borders = 60
        print '\n{border}\n{title}\n{border}\n'.format(
            border='-'*chars_in_borders,
            title=title
        )

    def write_to_file(self, data, filename):
        self._ensure_directory_exists(self.report_dir)

        full_path = os.path.join(self.report_dir, filename)
        with open(full_path, 'wb+') as fp:
            UnicodeWriter(fp).writerows(data)

    def _ensure_directory_exists(self, directory):
        if not os.path.exists(directory):
            os.makedirs(directory)


if __name__ == '__main__':
    builder = ReportBuilder()
    builder.print_header("Events Summary")
    builder.summary()

    builder.print_header("Attendee Summary")
    builder.attendee_breakdown()

    builder.print_header("Full Report")
    builder.event_tables()
