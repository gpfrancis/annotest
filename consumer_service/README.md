# Consumer Service
A simple Kafka consumer designed to be run continuously, e.g. as a service.
Reads from a specified topic and appends to an output file.

Usage: `consumer_service.py --out=<file> --topic=<topic> [--broker=<broker>] [--group=<id>]`

Options:
```
    --out=<file>        File to append output to
    --topic=<topic>     Kafka topic to read from
    --broker=<broker>   Kafka broker (default is lasair public kafka)
    --group=<id>        Group ID (default is random)
```

## Installation

### As a Systemd service

### As a Docker container
