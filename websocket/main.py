from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from aiokafka import AIOKafkaProducer, AIOKafkaConsumer
import asyncio
import json

app = FastAPI()

# Kafka 설정
KAFKA_BROKER_URL = "kafka:9092"
KAFKA_TOPIC = "chat-1"

# WebSocket 연결 관리
class WebSocketManager:
    def __init__(self):
        self.active_connections = []

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)

    def disconnect(self, websocket: WebSocket):
        self.active_connections.remove(websocket)

    async def send_message(self, message: str):
        for connection in self.active_connections:
            await connection.send_text(message)

manager = WebSocketManager()

# WebSocket 엔드포인트
@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await manager.connect(websocket)
    try:
        while True:
            data = await websocket.receive_text()
            await producer.send_and_wait(KAFKA_TOPIC, json.dumps({"message": data}).encode("utf-8"))
    except WebSocketDisconnect:
        manager.disconnect(websocket)

# Kafka Consumer 메시지 처리
async def consume_kafka_messages():
    consumer = AIOKafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BROKER_URL,
        group_id="websocket_group",
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    )
    await consumer.start()
    try:
        async for msg in consumer:
            message = msg.value["message"]
            await manager.send_message(message)
    finally:
        await consumer.stop()

# Kafka Producer 및 Consumer 초기화
@app.on_event("startup")
async def startup_event():
    global producer
    producer = AIOKafkaProducer(
        bootstrap_servers=KAFKA_BROKER_URL,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )
    await producer.start()
    asyncio.create_task(consume_kafka_messages())

@app.on_event("shutdown")
async def shutdown_event():
    await producer.stop()
