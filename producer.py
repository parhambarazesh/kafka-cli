#!/usr/bin/env python3
"""
Standalone Kafka Producer
Run this to send messages to the demo-topic
"""

import time
import sys
from confluent_kafka import Producer

def create_producer():
    return Producer({
        "bootstrap.servers": "localhost:9092",
        "acks": "all",  # Wait for all replicas to acknowledge
        "retries": 3,
        "batch.size": 16384,
        "linger.ms": 10,
    })

def send_message(producer, topic, message, key=None):
    """Send a message to Kafka topic"""
    try:
        producer.produce(
            topic=topic,
            key=key,
            value=message,
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

def main():
    topic = "demo-topic"
    producer = create_producer()

    print("Kafka Producer started!")
    print(f"Sending messages to topic: {topic}")
    print("-" * 50)

    try:
        if len(sys.argv) > 1:
            # Send message from command line argument
            message = " ".join(sys.argv[1:])
            timestamp = int(time.time())
            key = f"key-{timestamp}"

            print(f"Sending: {message}")
            success = send_message(producer, topic, message, key)

            if success:
                print("Message sent successfully!")
            else:
                print("❌ Failed to send message")
        else:
            # Interactive mode
            print("Interactive mode - type messages (Ctrl+C to exit):")
            counter = 1

            while True:
                try:
                    message = input(f"Message #{counter}: ")
                    if message.strip():
                        timestamp = int(time.time())
                        key = f"key-{timestamp}"

                        success = send_message(producer, topic, message, key)
                        if success:
                            print(f"✅ Sent message #{counter}")
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
    main()
