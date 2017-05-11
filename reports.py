import os
from terminaltables import AsciiTable
from my_events.config import NEO4J_CONNECTION
from my_events.csv_writer import UnicodeWriter
from my_events.db import Neo4J


class ReportBuilder(object):
    def __init__(self, report_dir='reports'):
        self.neo = Neo4J(NEO4J_CONNECTION)
        self.report_dir = report_dir

    def summary(self):
        table_data = [["Event", "Date", "Attendees"]]

        data = self.neo.db.data(
            """
            MATCH (e:Event)
            OPTIONAL MATCH (e)<-[:ATTENDED*]-(a:Attendee)
            WITH e,count(a) as attendees
            RETURN e.name as name, e.date as date, attendees"""
        )

        table_data.extend(
            [
                [event['name'], event['date'], event['attendees']]
                for event in data
            ]
        )

        table = AsciiTable(table_data)
        print table.table

    def attendee_breakdown(self):
        data = self.neo.db.data(
            """
            MATCH (a:Attendee)-[:ATTENDED*]->(e:Event)
            with a, collect(e) as events
            RETURN a.name as name, events
            """
        )

        for attendee in data:
            table_data = [[attendee['name'], "Event", "Category"]]
            table_data.extend([
                ['', event['name'], event['category']]
                for event in attendee['events']
            ])

            table = AsciiTable(table_data)
            print table.table
            print '\n'

    def event_tables(self):
        events = self.neo.db.data(
            """
            MATCH (e:Event)
            OPTIONAL MATCH (e)<-[:ATTENDED*]-(a:Attendee)
            WITH e,collect(a) as attendees
            RETURN e.name as name, e.date as date, attendees"""
        )


        for event in events:
            print '{name}: {date}'.format(**event)
            table_data = [["Name", "Twitter", "Website", "Company"]]

            table_data.extend([
                [attendee.get('name', ''), attendee.get('twitter', ''), attendee.get('website', ''), attendee.get('company', '')]
                for attendee in event['attendees']
            ])

            table = AsciiTable(table_data)
            print table.table
            print '\n'

            date = '-'.join(reversed(event['date'].split('/')))

            self.write_to_file(table_data, '{name}-{date}.csv'.format(
                name=event['name'].replace(' ', '_'),
                date=date
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
