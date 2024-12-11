from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from aiokafka import AIOKafkaProducer, AIOKafkaConsumer
from fastapi.middleware.cors import CORSMiddleware
from websocket_manager import ChatManager
import asyncio
import json

app = FastAPI()
KAFKA_BROKER_URL = "kafka:9092"

# CORS 설정
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # 모든 도메인 허용
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

manager = ChatManager()

# Kafka Producer 초기화
producer = AIOKafkaProducer(
    bootstrap_servers=KAFKA_BROKER_URL,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

@app.websocket("/ws/{user1}/{user2}")
async def websocket_endpoint(websocket: WebSocket, user1: str, user2: str):
    """WebSocket을 통해 실시간 채팅을 처리"""
    KAFKA_TOPIC = f"{min(user1, user2)}-{max(user1, user2)}"
    print(f"Connected to topic: {KAFKA_TOPIC}")

    # WebSocket 연결 관리
    await manager.connect(websocket, user1, user2)

    # Kafka Consumer 설정
    consumer = AIOKafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BROKER_URL,
        group_id=f"{user1}_group",
        auto_offset_reset="earliest",
        value_deserializer=lambda x: json.loads(x.decode("utf-8")),
    )
    await consumer.start()

    async def consume_messages():
        """Kafka에서 메시지를 읽어 WebSocket으로 전송"""
        try:
            async for msg in consumer:
                message_data = msg.value
                try:
                    await websocket.send_text(f"{message_data['sender']}: {message_data['message']}")
                except RuntimeError:
                    # WebSocket이 닫힌 경우 루프 종료
                    print("WebSocket closed during sending message")
                    break
        finally:
            await consumer.stop()

    async def produce_messages():
        """WebSocket에서 메시지를 읽어 Kafka로 전송"""
        try:
            while True:
                try:
                    data = await websocket.receive_text()
                    await producer.send_and_wait(
                        KAFKA_TOPIC,
                        key=f"{user1}:{KAFKA_TOPIC}".encode(),
                        value={"sender": user1, "message": data},
                    )
                except WebSocketDisconnect:
                    print("WebSocket disconnected during receiving message")
                    break
        finally:
            await manager.disconnect(websocket, user1, user2)

    try:
        # Kafka Consumer와 WebSocket 송신 병렬 실행
        await asyncio.gather(consume_messages(), produce_messages())
    finally:
        # WebSocket 연결 종료 보장
        await websocket.close()

@app.on_event("startup")
async def startup_event():
    """Kafka Producer 시작"""
    await producer.start()

@app.on_event("shutdown")
async def shutdown_event():
    """Kafka Producer 종료"""
    await producer.stop()
