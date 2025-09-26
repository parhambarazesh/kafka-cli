"""Kafka Producer Tool"""
"""
Example usage:
    python producer.py --publish-to kafka --topic demo-topic --partition 0
    python producer.py --publish-to eventhub --topic test-event-hub partition 1
This script allows sending messages to a specified Kafka topic interactively.
Messages can be sent to a specific partition if desired.
Configuration is read from a JSON file (config.json) which should contain
the necessary connection details for Kafka or Event Hub.
"""

import time
import sys
import json
from argparse import ArgumentParser
from confluent_kafka import Producer


def get_config(file_path):
    with open(file_path, 'r') as file:
        config_data = json.load(file)
    return config_data


def send_message(producer, topic, message, key=None):
    kwargs = {
        "topic": topic,
        "key": key,
        "value": message,
        "callback": delivery_callback,
    }
    if args.partition is not None:
        kwargs["partition"] = args.partition

    try:
        producer.produce(**kwargs)
        producer.flush()  # Wait for message to be delivered
        return True
    except Exception as e:
        print(f"❌ Failed to send message: {e}")
        return False


def delivery_callback(err, msg):
    # Callback for message delivery confirmation
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()}[{msg.partition()}] at offset {msg.offset()}")


def main(config):
    if "--help" in sys.argv or "-h" in sys.argv:
        parser.print_help()
        return

    topic = args.topic
    producer = Producer(config.get("config"))

    print(
        f"Kafka Producer started!\n"
        f"Sending messages to topic: {topic}\n"
        f"{'-' * 50}"
    )

    try:
        print("Interactive mode - type messages (Ctrl+C to exit):")
        counter = 1

        while True:
            try:
                message = input(f"Message #{counter}: ")
                if message.strip():
                    timestamp = int(time.time())
                    if args.static_key:
                        key = "static-key"
                    else:
                        key = f"key-{timestamp}"

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
    parser.add_argument("--publish-to", help="target environment to publish messages to; kafka or eventhub",
                        default="kafka")
    parser.add_argument("--partition", type=int, help="Specific partition to produce to", default=None)
    parser.add_argument("--topic", type=str, help="Topic to produce to", default="demo-topic")
    parser.add_argument("--static-key", action="store_true",
                        help="Use a static key for all messages (for testing partitioning)")
    args = parser.parse_args()
    environment = args.publish_to

    conf = get_config("config.json").get("producer").get(environment)
    if conf is None:
        print(f"Environment '{environment}' not found in config.json. Exiting.")
        sys.exit(1)

    main(conf)
