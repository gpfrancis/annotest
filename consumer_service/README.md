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

## Notes

This is intended as example code to be built on rather than a fully production
grade service. For example, it does not consider error handling or logging.

You probably want to start by replacing `handle_alerts` with your own version
that does something more useful than simply writing it to an ever-growing file.

We deal with the alerts in small batches here rather than singly because
that's a common pattern and, depending on what you're doing with the alerts, might
be quite a lot more efficient. For a case as simple as this it doesn't really
make sense and you could simlify the code somewhat and process the alerts one at a
time if you wanted to.

## Installation

### As a Systemd service
The file `service.yaml` is an Ansible playbook that will install the consumer service as a systemd daemon
on the local machine. 

Edit it to change the default values for various configuration parameters as required.
You will at least need to set the variables `group` and `topic`. Then
run the command:
```
ansible-playbook service.yaml
```

### As a Docker container
