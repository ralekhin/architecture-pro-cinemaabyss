import os
import uuid
import datetime as dt
import json
import asyncio
from typing import Any, Dict
import logging
from contextlib import asynccontextmanager
from fastapi import FastAPI, Request, Response, status
from kafka import KafkaProducer, KafkaConsumer
from kafka.errors import KafkaError


@asynccontextmanager
async def lifespan(app: FastAPI):
    print("Application starting up...")
    loop = asyncio.get_event_loop()
    for topic in ["movie-events", "user-events", "payment-events"]:
        loop.create_task(asyncio.to_thread(lambda t=topic: asyncio.run(consume(t))))
    yield
    print("Application shutting down...")
    producer.close()
app = FastAPI(title="Events service", version="1.0.0", lifespan=lifespan)

PORT = int(os.getenv("PORT", "8082"))
KAFKA_BROKERS = os.getenv("KAFKA_BROKERS", "kafka:9092")

logging.basicConfig(level=logging.INFO)

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKERS.split(","),
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    retries=3,
)

@app.get("/api/events/health", tags=["health"])
async def health():
    return {"status": True}

async def consume(topic: str):
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=KAFKA_BROKERS.split(","),
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        group_id=f"events-service-{topic}-group",
        auto_offset_reset="earliest",
    )
    for message in consumer:
        logging.info(f"Consumed topic {topic}: {message.value}")

@app.post("/api/events/movie", status_code=status.HTTP_201_CREATED)
async def create_movie_event(request: Request):
    body = await request.json()
    return _send("movie-events", body)

@app.post("/api/events/user", status_code=status.HTTP_201_CREATED)
async def create_user_event(request: Request):
    body = await request.json()
    return _send("user-events", body)

@app.post("/api/events/payment", status_code=status.HTTP_201_CREATED)
async def create_payment_event(request: Request):
    body = await request.json()
    return _send("payment-events", body)

def _send(topic: str, payload: Dict[str, Any]):
    event = {
        "id": str(uuid.uuid4()),
        "type": topic.split("-")[0],
        "timestamp": dt.datetime.now(dt.UTC).isoformat() + "Z",
        "payload": payload,
    }
    try:
        logging.info(f"Produced topic {topic}: {event}")
        return {
            "status": "success",
            "event": event,
        }
    except KafkaError as e:
        return {"status": "error", "error": str(e)}
