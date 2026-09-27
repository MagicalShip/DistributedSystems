import pika


TASK_QUEUE = "task_queue"
DEAD_LETTER_EXCHANGE = "dead_letter_exchange"
DEAD_LETTER_QUEUE = "dead_letter_queue"
DEAD_LETTER_ROUTING_KEY = "dead_letter"


def declare_topology(channel: pika.adapters.blocking_connection.BlockingChannel) -> None:
    channel.exchange_declare(
        exchange=DEAD_LETTER_EXCHANGE,
        exchange_type="direct",
        durable=True,
    )
    channel.queue_declare(queue=DEAD_LETTER_QUEUE, durable=True)
    channel.queue_bind(
        exchange=DEAD_LETTER_EXCHANGE,
        queue=DEAD_LETTER_QUEUE,
        routing_key=DEAD_LETTER_ROUTING_KEY,
    )
    channel.queue_declare(
        queue=TASK_QUEUE,
        durable=True,
        arguments={
            "x-dead-letter-exchange": DEAD_LETTER_EXCHANGE,
            "x-dead-letter-routing-key": DEAD_LETTER_ROUTING_KEY,
        },
    )
