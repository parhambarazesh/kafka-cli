#!/usr/bin/env python3
"""
Standalone Kafka Producer
Run this to send messages to the demo-topic
"""

import time
import sys
from argparse import ArgumentParser

from confluent_kafka import Producer
import json


def get_config(file_path):
    with open(file_path, 'r') as file:
        config_data = json.load(file)
    return config_data


def send_message(producer, topic, message, key=None):
    """Send a message to Kafka topic"""
    try:
        producer.produce(
            topic=topic,
            key=key,
            value=message,
            # partition=2, # Send to partition
            callback=delivery_callback
        )
        producer.flush()  # Wait for message to be delivered
        return True
    except Exception as e:
        print(f"❌ Failed to send message: {e}")
        return False


def delivery_callback(err, msg):
    """Callback for message delivery confirmation"""
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()}[{msg.partition()}] at offset {msg.offset()}")


def main(config):
    topic = config.get("topic")
    producer = Producer(config.get("config"))

    print("Kafka Producer started!")
    print(f"Sending messages to topic: {topic}")
    print("-" * 50)

    try:
        print("Interactive mode - type messages (Ctrl+C to exit):")
        counter = 1

        while True:
            try:
                message = input(f"Message #{counter}: ")
                if message.strip():
                    timestamp = int(time.time())
                    key = f"key-{timestamp}"
                    # key = "static-key"  # Uncomment to use a static key for partitioning

                    success = send_message(producer, topic, message, key)
                    if success:
                        print(f"✅ Sent message #{counter} with key '{key}'")
                        counter += 1
                    else:
                        print("❌ Failed to send message")

            except EOFError:
                break

    except KeyboardInterrupt:
        print("\nProducer stopped by user")

    finally:
        producer.flush()
        print("Producer closed")


if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument("--consume-from", help="kafka or eventhub", default="kafka")
    args = parser.parse_args()
    environment = args.consume_from

    conf = get_config("config.json").get("producer").get(environment)
    if conf is None:
        print(f"Environment '{environment}' not found in config.json. Exiting.")
        sys.exit(1)

    main(conf)
