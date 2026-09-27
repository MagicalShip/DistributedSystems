import json
import uuid
from datetime import datetime, timezone

import pika
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

from messaging import TASK_QUEUE, declare_topology
from storage import get_result, save_result


app = FastAPI(title="Async AI Processing Service")


class ProcessRequest(BaseModel):
    text: str = Field(min_length=1)


@app.post("/process")
def create_process(request: ProcessRequest):
    request_id = str(uuid.uuid4())
    message = {
        "id": request_id,
        "text": request.text,
        "timestamp": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "retry_count": 0,
    }

    save_result(
        request_id,
        {"id": request_id, "status": "processing"},
    )

    connection = None
    try:
        connection = pika.BlockingConnection(
            pika.ConnectionParameters(host="localhost", port=5672)
        )
        channel = connection.channel()
        declare_topology(channel)
        channel.basic_publish(
            exchange="",
            routing_key=TASK_QUEUE,
            body=json.dumps(message),
            properties=pika.BasicProperties(delivery_mode=2),
        )
    except pika.exceptions.AMQPError as error:
        save_result(
            request_id,
            {"id": request_id, "status": "error", "error": str(error)},
        )
        raise HTTPException(status_code=503, detail="RabbitMQ is unavailable") from error
    finally:
        if connection is not None and connection.is_open:
            connection.close()

    return {"id": request_id, "status": "processing"}


@app.get("/result/{request_id}")
def read_result(request_id: str):
    result = get_result(request_id)

    if result is None:
        raise HTTPException(status_code=404, detail="Request ID not found")

    return result
