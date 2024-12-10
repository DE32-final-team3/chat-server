from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException
from aiokafka import AIOKafkaProducer, AIOKafkaConsumer
from fastapi.middleware.cors import CORSMiddleware
from websocket_manager import ChatManager
import asyncio
import json

app = FastAPI()
KAFKA_BROKER_URL = "kafka:9092"

# CORS 설정 추가
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # 모든 도메인 허용 (보안을 위해 필요한 도메인만 명시적으로 추가하세요)
    allow_credentials=True,
    allow_methods=["*"],  # 모든 HTTP 메서드 허용
    allow_headers=["*"],  # 모든 헤더 허용
)

manager = ChatManager()

# Producer 인스턴스 초기화
producer = AIOKafkaProducer(
    bootstrap_servers=KAFKA_BROKER_URL,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

@app.websocket("/ws/{user1}/{user2}")
async def websocket_endpoint(websocket: WebSocket, user1: str, user2: str):
    KAFKA_TOPIC = "-".join(sorted([user1, user2])) #f"{min(user1, user2)}-{max(user1, user2)}"
    print(f"Connected to topic: {KAFKA_TOPIC}")

    await manager.connect(websocket, user1, user2)
    try:
        consumer = AIOKafkaConsumer(
            KAFKA_TOPIC,
            bootstrap_servers=KAFKA_BROKER_URL,
            group_id=f"{user1}_group",
            auto_offset_reset="earliest",
            value_deserializer=lambda x: json.loads(x.decode("utf-8")),
        )
        await consumer.start()

        try:
            async for msg in consumer:
                message_data = msg.value
                print(f"check : {message_data}")
                await websocket.send_text(f"{message_data['sender']}: {message_data['message']}")
        finally:
            await consumer.stop()

        while True:
            data = await websocket.receive_text()
            await producer.send_and_wait(
                KAFKA_TOPIC,
                key=f"{user1}:{KAFKA_TOPIC}".encode(),
                value={"sender": user1, "message": data},
            )
    except WebSocketDisconnect:
        await manager.disconnect(websocket, user1, user2)


@app.post("/send/{topic}")
async def send_message(topic: str, sender: str, receiver: str, message: str):
    topic = "-".join(sorted([sender, receiver])) #f"{min(sender, receiver)}-{max(sender, receiver)}"
    try:
        await producer.send_and_wait(
            topic,
            key=f"{sender}:{topic}".encode(),
            value={"sender": sender, "message": message},
        )
        return {"status": "Message sent successfully", "topic": topic, "message": message}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.on_event("startup")
async def startup_event():
    await producer.start()


@app.on_event("shutdown")
async def shutdown_event():
    await producer.stop()
