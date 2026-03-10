"""
A simple Kafka annotator designed to be run continuously, e.g. as a service.
Reads from a specified topic and classifies objects as fruit.
Usage:
    fruitbowl.py --token=<token> --topic=<topic> --annotator=<topic> [--broker=<broker>] [--group=<id>]

Options:
    --token=<token>     Lasair API token
    --topic=<topic>     Kafka topic to read from
    --annotator=<topic> Annotator topic
    --broker=<broker>   Kafka broker (default is lasair public kafka)
    --group=<id>        Group ID (default is random)
"""

from docopt import docopt
from confluent_kafka import Consumer
import signal
from threading import Event
from uuid import uuid4
import json
import random
import lasair

# Configuration
batch_size = 10
fruit = ['apple', 'pear', 'orange', 'banana']

stop = Event()


def stop_handler(signum, _frame):
    print(f'Stopping consumer service on {signal.Signals(signum).name}')
    stop.set()


class FruitBowl:

    def __init__(self, token, topic, annotator, broker, group):
        self.annotator = annotator
        self.consumer = None
        consumer_conf = {
            'bootstrap.servers': broker,
            'default.topic.config': {'auto.offset.reset': 'earliest'},
            'client.id': 'client-1',
            'group.id': group,
            'enable.auto.commit': False,
        }
        self.consumer = Consumer(consumer_conf)
        self.consumer.subscribe([topic])
        print(f"Reading topic {topic} from {broker}")
        self.lc = lasair.lasair_client(token)
        print(f"Annotating to topic {annotator}")

    def handle_alerts(self, alerts):
        for alert in alerts:
            objectid = int(json.loads(alert)['diaObjectId'])
            random.seed(objectid)
            classification = fruit[random.randrange(0, len(fruit))]
            self.lc.annotate(
                self.annotator,
                objectid,
                classification,
                version='0.1',
                explanation='Random',
                classdict={},
                url='')

    def run(self):
        while not stop.is_set():
            print('Polling for alerts')
            alerts = []
            nalert = 0
            while nalert < batch_size:
                if stop.is_set():
                    break
                msg = self.consumer.poll(timeout=2)
                if msg is None:
                    # no messages available
                    break
                if msg.error():
                    print('ERROR polling for alerts: ' + str(msg.error()))
                    break
                alerts.append(msg.value())
                nalert += 1
            print(f'Got {nalert} alerts')
            if nalert > 0:
                # handle a batch of alerts
                self.handle_alerts(alerts)
                # commit offsets
                self.consumer.commit()
            else:
                stop.wait(10)
        # clean up
        self.consumer.commit()
        self.consumer.close()
        self.out.close()


if __name__ == '__main__':
    args = docopt(__doc__)
    signal.signal(signal.SIGTERM, stop_handler)
    signal.signal(signal.SIGINT, stop_handler)
    signal.signal(signal.SIGHUP, stop_handler)
    fb = FruitBowl(
        args['--token'],
        args['--topic'],
        args['--annotator'],
        args.get('--broker') or 'lasair-lsst-kafka_pub.lsst.ac.uk:9092',
        args.get('--group') or str(uuid4()))
    fb.run()
