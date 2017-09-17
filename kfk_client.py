#!/usr/bin/env python3

"""A simple python script template.
"""

from __future__ import print_function
import sys
import argparse
import threading
import logging
import time
import multiprocessing

from confluent_kafka import Producer, Consumer

__version__ = (0, 1, 0)

"""
logger object creation that will allow us to write logs.
"""
logger = logging.getLogger()
# on met le niveau du logger à DEBUG, comme ça il écrit tout
logger.setLevel(logging.ERROR)
# création d'un formateur qui va ajouter le temps, le niveau
# de chaque message quand on écrira un message dans le log
formatter = logging.Formatter('%(levelname)s - %(message)s')
# création d'un second handler qui va rediriger chaque écriture de log
# sur la console
stream_handler = logging.StreamHandler(stream=sys.stdout)
stream_handler.setLevel(logging.ERROR)
stream_handler.setFormatter(formatter)
logger.addHandler(stream_handler)


class Producer(threading.Thread):
    daemon = True

    def run(self):
        producer = Producer(bootstrap_servers='localhost:9092')

        while True:
            producer.send('my-topic', b"test")
            producer.send('my-topic', b"\xc2Hola, mundo!")
            time.sleep(1)


class Consumer(multiprocessing.Process):
    daemon = True

    def run(self):
        consumer = Consumer(bootstrap_servers='localhost:9092',
                                 auto_offset_reset='earliest')
        consumer.subscribe(['my-topic'])

        for message in consumer:
            print(message)


def main():
    tasks = [
        Producer(),
        Consumer()
    ]

    for t in tasks:
        t.start()

    time.sleep(10)

def get_version():
    return '.'.join(map(str, __version__))

def main(arguments):
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('infile', help="Input file", type=argparse.FileType('r'))
    parser.add_argument('-o', '--outfile', help="Output file",
                        default=sys.stdout, type=argparse.FileType('w'))

    args = parser.parse_args(arguments)

    print(args)


if __name__ == '__main__':
    sys.exit(main(sys.argv[1:]))
