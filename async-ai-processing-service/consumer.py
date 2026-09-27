import json
import os
import time
from datetime import datetime, timezone

import pika

from messaging import TASK_QUEUE, declare_topology


PROCESSING_DELAY_SECONDS = float(os.getenv("PROCESSING_DELAY_SECONDS", "1"))


def process_message(channel, method, properties, body):
    message = json.loads(body)
    processing_time = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

    print(
        f"Processing id={message['id']}, "
        f"text={message['text']!r}, "
        f"at={processing_time}, "
        f"redelivered={method.redelivered}"
    )

    # Slow processing down so the queue depth can be observed.
    time.sleep(PROCESSING_DELAY_SECONDS)

    # Acknowledge only after processing has completed.
    channel.basic_ack(delivery_tag=method.delivery_tag)


connection = pika.BlockingConnection(
    pika.ConnectionParameters(host="localhost", port=5672)
)
channel = connection.channel()
declare_topology(channel)

# Give this consumer only one unacknowledged message at a time.
channel.basic_qos(prefetch_count=1)
channel.basic_consume(
    queue=TASK_QUEUE,
    on_message_callback=process_message,
    auto_ack=False,
)

print("Waiting for messages. Press Ctrl+C to stop.")

try:
    channel.start_consuming()
except KeyboardInterrupt:
    channel.stop_consuming()
finally:
    connection.close()
