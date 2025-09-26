"""Kafka Consumer Script"""
"""
Example usage:
    python consumer.py --consume-from kafka --topic demo-topic --from-beginning --partition 0
    python consumer.py --consume-from eventhub --topic test-event-hub --from-beginning --fixed-group --partition 2
    python consumer.py --consume-from eventhub --topic test-event-hub
This script allows consuming messages from a specified Kafka topic interactively.
Messages can be consumed from a specific partition if desired.
Configuration is read from a JSON file (config.json) which should contain
the necessary connection details for Kafka or Event Hub.
"""
import json
import uuid
import time
import sys
from argparse import ArgumentParser

from confluent_kafka import Consumer, TopicPartition

def get_config(file_path):
    with open(file_path, 'r') as file:
        config_data = json.load(file)
    return config_data

def create_consumer(config, group_id=None, start_from="latest"):
    if group_id is None:
        # Use unique group ID to always start fresh
        group_id = f"consumer-{int(time.time())}-{uuid.uuid4().hex[:8]}"

    for key, value in config.items():
        if isinstance(value, str) and value.startswith("{{") and value.endswith("}}"):
            var = value[2:-2].strip()
            if var == "group_id":
                config[key] = group_id
            elif var == "start_from":
                config[key] = start_from

    return Consumer(config), group_id

def consume_messages(consumer, topic, mode="continuous"):
    if args.partition is not None:
        tp = TopicPartition(topic, args.partition)
        consumer.assign([tp])
    else:
        consumer.subscribe([topic])
        print(f"Subscribed to topic: {topic}")

    if mode == "latest_only":
        print("Positioning at end of topic to catch only new messages...")
        for _ in range(3):  # Poll a few times to reach the end
            msg = consumer.poll(timeout=1.0)
            if msg and not msg.error():
                print(f"Skipping existing: {msg.value().decode()}")
        print("Ready for new messages!")

    message_count = 0

    try:
        while True:
            msg = consumer.poll(timeout=1.0)

            if msg is None:
                continue

            if msg.error():
                print(f"\n Consumer error: {msg.error()}")
                continue

            # Process the message
            message_count += 1
            key = msg.key().decode() if msg.key() else "None"
            value = msg.value().decode()

            print(f"\nMessage #{message_count}:\n"
                  f"   Key: {key}\n"
                  f"   Value: {value}\n"
                  f"   Partition: {msg.partition()}\n"
                  f"   Offset: {msg.offset()}\n"
                  f"   Timestamp: {msg.timestamp()[1] if msg.timestamp()[1] > 0 else 'N/A'}")


            if mode == "single":
                print("Single message mode - exiting after first message")
                break

    except KeyboardInterrupt:
        print(f"\nConsumer stopped by user after {message_count} messages")

    return message_count

def main(config):
    topic = args.topic
    config = config.get("config")

    if "--help" in sys.argv or "-h" in sys.argv:
        parser.print_help()
        return

    if args.fixed_group:
        group_id = "my-fixed-consumer-group"
    else:
        group_id = None

    if args.from_beginning: # Read all messages from the latest position
        start_from = "earliest"
        mode = "continuous"
    elif args.latest_only:
        start_from = "latest"
        mode = "latest_only"
    elif args.single:
        start_from = "latest"
        mode = "single"
    else:
        start_from = "latest"
        mode = "continuous"

    consumer, actual_group_id = create_consumer(config, group_id, start_from)

    print(f"Kafka Consumer started!\n"
          f"Consumer Group ID: {actual_group_id}\n"
          f"Topic: {topic}\n"
          f"{'-' * 60}")

    try:
        message_count = consume_messages(consumer, topic, mode)
        print(f"\nTotal messages processed: {message_count}")

    finally:
        consumer.close()
        print("Consumer closed")

if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument("--from-beginning", action="store_true", help="Read all messages from the beginning")
    parser.add_argument("--latest-only", action="store_true", help="Only read new messages")
    parser.add_argument("--fixed-group", action="store_true", help="Use a fixed consumer group (remembers position)")
    parser.add_argument("--single", action="store_true", help="Read a single message and exit")
    parser.add_argument("--consume-from", help="local or eventhub", default="local")
    parser.add_argument("--partition", type=int, help="Specific partition to consume from", default=None)
    parser.add_argument("--topic", type=str, help="Topic to consume to", default="demo-topic")
    args = parser.parse_args()

    environment = args.consume_from

    conf = get_config("config.json").get("consumer").get(environment)
    if conf is None:
        print(f"Environment '{environment}' not found in config.json. Exiting.")
        sys.exit(1)

    main(conf)
