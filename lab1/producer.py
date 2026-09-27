import json
from datetime import datetime, timezone

import pika

from messaging import TASK_QUEUE, declare_topology


# Connect to RabbitMQ through its program port.
connection = pika.BlockingConnection(
    pika.ConnectionParameters(host="localhost", port=5672)
)
channel = connection.channel()

# Declaring the same topology is safe when it already exists.
declare_topology(channel)

for message_id in range(1, 11):
    message = {
        "id": message_id,
        "text": "Process this request",
        "timestamp": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
    }

    channel.basic_publish(
        exchange="",
        routing_key=TASK_QUEUE,
        body=json.dumps(message),
        properties=pika.BasicProperties(delivery_mode=2),
    )
    print(f"Sent message {message_id}")

connection.close()
