import sys
from my_events.ingester import CSVStreamIngester


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

    CSVStreamIngester(data_type).process(sys.stdin, has_headers=True)