from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from aiokafka import AIOKafkaProducer, AIOKafkaConsumer
from fastapi.middleware.cors import CORSMiddleware
from websocket_manager import ChatManager
import asyncio
import json
from pymongo import MongoClient
from datetime import datetime

# MongoDB 설정
client = MongoClient("mongodb://root:team3@172.17.0.1:27017/?authSource=admin")
db = client["chat_db"]  # 데이터베이스 이름 설정

# MongoDB에 메시지 저장 함수
def save_message_to_mongo(message, topic, time):
    room_collection = db[topic]  # 채팅방 이름에 맞는 컬렉션 사용
    converted_time = datetime.fromtimestamp(time / 1000)
    room_collection.insert_one({
        "user_id": message['sender'],
        "message": message['message'],
        "timestamp": converted_time
    })

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

    # 사용자 ID를 정렬해서 일관된 Kafka Topic과 그룹 ID 생성
    sorted_users = sorted([user1, user2])
    room_user1, room_user2 = sorted_users
    KAFKA_TOPIC = f"{room_user1}-{room_user2}"
    KAFKA_GROUP_ID = f"{room_user1}-{room_user2}_group"  # 고유한 그룹 ID

    print(f"Connected to topic: {KAFKA_TOPIC}")

    # WebSocket 연결 관리
    await manager.connect(websocket, room_user1, room_user2)

    # Kafka Consumer 설정
    consumer = AIOKafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BROKER_URL,
        group_id=KAFKA_GROUP_ID,
        auto_offset_reset="earliest",
        value_deserializer=lambda x: json.loads(x.decode("utf-8")),
    )
    await consumer.start()

    async def consume_messages():
        """Kafka에서 메시지를 읽어 WebSocket으로 전송"""
        try:
            async for msg in consumer:
                message_data = msg.value
                time_data = msg.timestamp
                # 채팅방의 모든 WebSocket 연결에 메시지 브로드캐스트
                await manager.send_message(
                    f"{message_data['sender']}: {message_data['message']}",
                    room_user1,
                    room_user2
                )
                save_message_to_mongo(message_data, KAFKA_TOPIC, time_data)  # MongoDB 저장
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
                        key=f"{room_user1}:{KAFKA_TOPIC}".encode(),
                        value={"sender": user1, "message": data},
                    )
                except WebSocketDisconnect:
                    print("WebSocket disconnected during receiving message")
                    break
        finally:
            await manager.disconnect(websocket, room_user1, room_user2)

    try:
        # Kafka Consumer와 WebSocket 송신 병렬 실행
        await asyncio.gather(consume_messages(), produce_messages())
    finally:
        await websocket.close()

@app.on_event("startup")
async def startup_event():
    """Kafka Producer 시작"""
    await producer.start()

@app.on_event("shutdown")
async def shutdown_event():
    """Kafka Producer 종료"""
    await producer.stop()
