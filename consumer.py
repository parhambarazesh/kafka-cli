import uuid
import time
import sys
from confluent_kafka import Consumer, TopicPartition


def create_consumer(group_id=None, start_from="latest"):
    """Create a Kafka consumer with proper configuration"""

    if group_id is None:
        # Use unique group ID to always start fresh
        group_id = f"consumer-{int(time.time())}-{uuid.uuid4().hex[:8]}"

    config = {
        "bootstrap.servers": "localhost:9092",
        "group.id": group_id,
        "auto.offset.reset": start_from,  # "earliest" or "latest"
        "enable.auto.commit": True,
        "auto.commit.interval.ms": 1000,
        "session.timeout.ms": 30000,
        "heartbeat.interval.ms": 3000,
    }

    return Consumer(config), group_id

def consume_messages(consumer, topic, mode="continuous"):
    """Consume messages from Kafka topic"""

    consumer.subscribe([topic])
    print(f"Subscribed to topic: {topic}")

    # To consume a specific partition, uncomment below + comment the subscribe line above
    # tp = TopicPartition('demo-topic', 2)
    # consumer.assign([tp])

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

            print(f"\nMessage #{message_count}:")
            print(f"   Key: {key}")
            print(f"   Value: {value}")
            print(f"   Partition: {msg.partition()}")
            print(f"   Offset: {msg.offset()}")
            print(f"   Timestamp: {msg.timestamp()[1] if msg.timestamp()[1] > 0 else 'N/A'}")

            if mode == "single":
                print("Single message mode - exiting after first message")
                break

    except KeyboardInterrupt:
        print(f"\nConsumer stopped by user after {message_count} messages")

    return message_count

def main():
    topic = "demo-topic"

    if "--help" in sys.argv or "-h" in sys.argv:
        print("Kafka Consumer Usage:")
        print("  python consumer.py                    # Continuous mode with unique group")
        print("  python consumer.py --fixed-group      # Use fixed group (remembers position)")
        print("  python consumer.py --from-beginning   # Read all messages from start")
        print("  python consumer.py --latest-only      # Only new messages after start")
        print("  python consumer.py --single           # Read one message and exit")
        return

    if "--fixed-group" in sys.argv:
        group_id = "my-fixed-consumer-group"
        print("Using fixed consumer group (will remember position)")
    else:
        group_id = None
        print("Using unique consumer group (fresh start)")

    if "--from-beginning" in sys.argv: # Read all messages from the latest position
        start_from = "earliest"
        mode = "continuous"
        print("Will read from beginning of topic")
    elif "--latest-only" in sys.argv:
        start_from = "latest"
        mode = "latest_only"
        print("Will only read NEW messages")
    elif "--single" in sys.argv:
        start_from = "latest"
        mode = "single"
        print("Single message mode")
    else:
        start_from = "latest"
        mode = "continuous"
        print("Continuous mode from latest position")

    consumer, actual_group_id = create_consumer(group_id, start_from)

    print("Kafka Consumer started!")
    print(f"Consumer Group ID: {actual_group_id}")
    print(f"Topic: {topic}")
    print("-" * 60)

    try:
        message_count = consume_messages(consumer, topic, mode)
        print(f"\nTotal messages processed: {message_count}")

    finally:
        consumer.close()
        print("Consumer closed")

if __name__ == "__main__":
    main()
