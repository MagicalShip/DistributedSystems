import json
import os

import pika
import requests

from messaging import TASK_QUEUE, declare_topology
from storage import save_result


OLLAMA_URL = os.getenv(
    "OLLAMA_URL",
    "http://localhost:11434/api/generate",
)
OLLAMA_MODEL = os.getenv("OLLAMA_MODEL", "llama3.2:1b")
MAX_RETRIES = 3


def call_ollama(text: str) -> str:
    response = requests.post(
        OLLAMA_URL,
        json={
            "model": OLLAMA_MODEL,
            "prompt": text,
            "stream": False,
        },
        timeout=60,
    )
    response.raise_for_status()
    return response.json()["response"]


def process_message(channel, method, properties, body):
    message = json.loads(body)
    request_id = message["id"]

    print(f"Processing AI request id={request_id}")

    try:
        ai_result = call_ollama(message["text"])
        save_result(
            request_id,
            {
                "id": request_id,
                "status": "completed",
                "result": ai_result,
            },
        )
        channel.basic_ack(delivery_tag=method.delivery_tag)
        print(f"Completed AI request id={request_id}")
    except (requests.RequestException, KeyError, ValueError) as error:
        retry_count = message.get("retry_count", 0)

        if retry_count < MAX_RETRIES:
            message["retry_count"] = retry_count + 1
            channel.basic_publish(
                exchange="",
                routing_key=TASK_QUEUE,
                body=json.dumps(message),
                properties=pika.BasicProperties(delivery_mode=2),
            )
            channel.basic_ack(delivery_tag=method.delivery_tag)
            save_result(
                request_id,
                {
                    "id": request_id,
                    "status": "processing",
                    "retry_count": message["retry_count"],
                    "last_error": str(error),
                },
            )
            print(
                f"AI request failed id={request_id}; "
                f"retry {message['retry_count']}/{MAX_RETRIES}"
            )
        else:
            save_result(
                request_id,
                {
                    "id": request_id,
                    "status": "error",
                    "retry_count": retry_count,
                    "error": str(error),
                },
            )
            channel.basic_nack(
                delivery_tag=method.delivery_tag,
                requeue=False,
            )
            print(
                f"AI request permanently failed id={request_id}; "
                "sent to dead-letter queue"
            )


connection = pika.BlockingConnection(
    pika.ConnectionParameters(host="localhost", port=5672)
)
channel = connection.channel()
declare_topology(channel)
channel.basic_qos(prefetch_count=1)
channel.basic_consume(
    queue=TASK_QUEUE,
    on_message_callback=process_message,
    auto_ack=False,
)

print("AI consumer is waiting for messages. Press Ctrl+C to stop.")

try:
    channel.start_consuming()
except KeyboardInterrupt:
    channel.stop_consuming()
finally:
    connection.close()
