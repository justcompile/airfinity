from terminaltables import AsciiTable
from my_events.config import MONGO_CONNECTION_STRING
from my_events.db import Mongo


class ReportBuilder(object):
    def __init__(self):
        self.mongo = Mongo(MONGO_CONNECTION_STRING)

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

    def print_header(self, title):
        chars_in_borders = 60
        print '\n{border}\n{title}\n{border}\n'.format(
            border='-'*chars_in_borders,
            title=title
        )


if __name__ == '__main__':
    builder = ReportBuilder()
    builder.print_header("Events Summary")
    builder.summary()

    builder.print_header("Attendee Summary")
    builder.attendee_breakdown()

    builder.print_header("Full Report")
    builder.event_tables()