"""
A simple Kafka consumer designed to be run continuously, e.g. as a service.
Reads from a specified topic and appends to an output file.
Usage:
    consumer_service.py --topic=<topic> [--broker=<broker>] [--group=<id>]

Options:
    --topic=<topic>     Kafka topic to read from
    --broker=<broker>   Kafka broker (default is lasair public kafka)
    --group=<id>        Group ID (default is random)
"""

from docopt import docopt
from confluent_kafka import Consumer
from time import sleep
import signal
from threading import Event
import uuid

# Configuration
batch_size = 2

stop = Event()


def stop_handler(signum, _frame):
    print(f'Stopping consumer service on {signal.Signals(signum).name}')
    stop.set()


class ConsumerService:

    def __init__(self, broker, topic, group):
        self.consumer = None
        consumer_conf = {
            'bootstrap.servers': broker,
            'default.topic.config': {'auto.offset.reset': 'earliest'},
            'client.id': 'client-1',
            'group.id': group,
            'enable.auto.commit': False,
        }
        print(consumer_conf)
        self.consumer = Consumer(consumer_conf)
        self.consumer.subscribe([topic])
        print(f"Reading topic {topic} from {broker}")

    def run(self):
        while not stop.is_set():
            print('Polling for alerts')
            alerts = []
            nalert = 0
            while nalert < batch_size:
                if stop.is_set():
                    break
                msg = self.consumer.poll(timeout=5)
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
                print('---- Alert batch ----')
                for alert in alerts:
                    print(alert)
                print('---- end of batch ----')
                # commit offsets
                self.consumer.commit()
            else:
                stop.wait(10)
        # clean up - commit offsets and close the consumer
        self.consumer.commit()
        self.consumer.close()


if __name__ == '__main__':
    args = docopt(__doc__)
    signal.signal(signal.SIGTERM, stop_handler)
    signal.signal(signal.SIGINT, stop_handler)
    signal.signal(signal.SIGHUP, stop_handler)
    cs = ConsumerService(
        args.get('--broker') or 'lasair-lsst-kafka_pub.lsst.ac.uk:9092',
        args['--topic'],
        args.get('--group') or str(uuid.uuid4()))
    cs.run()
