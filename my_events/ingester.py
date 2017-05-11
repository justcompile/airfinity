from __future__ import print_function
import sys

from my_events.config import KAFKA_BROKERS


class Ingester(object):
    """
    Ingester objects are responsible for reading in data (iterable, streams etc)
    and send them to a Kafka Topic
    """
    def __init__(self, data_type):
        self._kafka_client = None

        if not data_type:
            raise RuntimeError('data_type must be specified')

        self.routing_key = data_type

    @property
    def kafka_client(self):
        from kafka import KafkaProducer
        if not self._kafka_client:
            self._kafka_client = KafkaProducer(
                bootstrap_servers=KAFKA_BROKERS
            )

        return self._kafka_client

    def process(self, data, **kwargs):
        raise NotImplementedError

    def remove_duplicates(self, data):
        return set(data)

    def _route_data(self, data_stream):
        try:
            for line in self.remove_duplicates(data_stream):
                if line:
                    self.kafka_client.send(
                        self.routing_key,
                        bytes(line.strip())
                    )

            # block until all async messages are sent
            self.kafka_client.flush()

        except TypeError:
            raise RuntimeError('data_stream is not iterable')


class CSVStreamIngester(Ingester):
    """
    CSVStreamIngester accepts a stream object container comma-seperated values
    and sends each row to a kafka topic
    """
    def process(self, data, has_headers=False, **kwargs):
        if has_headers:
            try:
                data.next()
            except AttributeError:
                data = data[1:]

        self._route_data(data)


def error(msg):
    sys.stderr.write(msg)
    sys.exit(1)


if __name__ == '__main__':
    try:
        format_type = sys.argv[1:][0]
    except IndexError:
        error('Error: Supply a data format. E.g. python {0} Alpha < file.csv\n'.format(sys.argv[0]))

    if sys.stdin.isatty():
        error('Error: Pipe the data into this application: python {0} data_type < file.csv\n'.format(sys.argv[0]))

    CSVStreamIngester(format_type).process(sys.stdin, has_headers=True)
