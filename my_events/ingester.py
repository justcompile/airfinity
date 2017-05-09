import sys


class Console(object):
    def send(self, key, data):
        sys.stdout.writelines(key+' - '+data)


class Ingester(object):
    def __init__(self, data_type):
        self._kafka_client = None

        if not data_type:
            raise RuntimeError('data_type must be specified')

        self.routing_key = data_type

    @property
    def kafka_client(self):
        if not self._kafka_client:
            self._kafka_client = Console()

        return self._kafka_client

    def send_data(self, data, **kwargs):
        raise NotImplementedError

    def _route_data(self, data_stream):
        try:
            for line in data_stream:
                self.kafka_client.send(self.routing_key, line)

        except TypeError:
            raise RuntimeError('data_stream is not iterable')


class CSVStreamIngester(Ingester):
    def send_data(self, data, has_headers=False, **kwargs):
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
        data_type = sys.argv[1:][0]
    except IndexError:
        error('Error: Supply a data format. E.g. python {0} Alpha < file.csv\n'.format(sys.argv[0]))

    if sys.stdin.isatty():
        error('Error: Pipe the data into this application: python {0} data_type < file.csv\n'.format(sys.argv[0]))

    CSVStreamIngester(data_type).send_data(sys.stdin, has_headers=True)