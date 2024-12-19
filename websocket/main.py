from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from aiokafka import AIOKafkaProducer, AIOKafkaConsumer
from aiokafka.admin import AIOKafkaAdminClient, NewTopic
from fastapi.middleware.cors import CORSMiddleware
from websocket_manager import ChatManager
import asyncio
import json
from pymongo import MongoClient
from datetime import datetime
from bson import ObjectId  

# MongoDB 설정
client = MongoClient("mongodb://root:cine@3.37.94.149:27017/?authSource=admin")
db = client["chat"]  # 데이터베이스 이름 설정

# Kafka 브로커 설정
KAFKA_BROKER_URL = "kafka:9092"
KAFKA_API_VERSION = "2.6.0"  # Kafka 브로커와 호환되는 API 버전

# FastAPI 초기화
app = FastAPI()

# CORS 설정
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

manager = ChatManager()

# Kafka Producer 초기화
producer = AIOKafkaProducer(
    bootstrap_servers=KAFKA_BROKER_URL,
    api_version=KAFKA_API_VERSION,  # Kafka API 버전 명시
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

# Kafka Admin Client 생성
async def ensure_topic_exists(topic_name: str):
    """Kafka 토픽 존재 여부 확인 및 생성"""
    admin_client = AIOKafkaAdminClient(
        bootstrap_servers=KAFKA_BROKER_URL,
        api_version=KAFKA_API_VERSION,  # Kafka API 버전 명시
    )
    try:
        existing_topics = await admin_client.list_topics()
        if topic_name not in existing_topics:
            await admin_client.create_topics([
                NewTopic(name=topic_name, num_partitions=1, replication_factor=1)
            ])
            print(f"Kafka topic '{topic_name}' created.")
        else:
            print(f"Kafka topic '{topic_name}' already exists.")
    except Exception as e:
        print(f"Error ensuring Kafka topic: {e}")
    finally:
        await admin_client.close()

# MongoDB 메시지 저장
def save_message_to_mongo(message, topic, time, offset):
    try:
        room_collection = db[topic]
        converted_time = datetime.fromtimestamp(time / 1000)
        room_collection.insert_one({
            "user_id": message['sender'],
            "message": message['message'],
            "timestamp": converted_time,
            "offset": offset
        })
        print(f"Message saved to MongoDB (Topic: {topic}, Offset: {offset}).")
    except Exception as e:
        print(f"Error saving message to MongoDB: {e}")

# MongoDB 채팅 기록 가져오기
def get_previous_messages(topic):
    try:
        room_collection = db[topic]
        return list(room_collection.find().sort("timestamp", 1))
    except Exception as e:
        print(f"Error retrieving messages: {e}")
        return []

# WebSocket 엔드포인트
@app.websocket("/ws/{user1}/{user2}")
async def websocket_endpoint(websocket: WebSocket, user1: str, user2: str):
    """WebSocket을 통한 실시간 채팅 처리"""
    sorted_users = sorted([user1, user2])
    KAFKA_TOPIC = f"{sorted_users[0]}-{sorted_users[1]}"
    KAFKA_GROUP_ID = f"{KAFKA_TOPIC}_group"

    # Kafka 토픽 존재 확인 및 생성
    await ensure_topic_exists(KAFKA_TOPIC)

    # WebSocket 연결 관리
    await manager.connect(websocket, user1, user2)

    # 이전 메시지 전송
    previous_messages = get_previous_messages(KAFKA_TOPIC)
    for msg in previous_messages:
        await websocket.send_text(f"{msg['user_id']}: {msg['message']}")

    # Kafka Consumer 설정
    consumer = AIOKafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BROKER_URL,
        api_version=KAFKA_API_VERSION,  # Kafka API 버전 명시
        group_id=KAFKA_GROUP_ID,
        auto_offset_reset="earliest",
        value_deserializer=lambda x: json.loads(x.decode("utf-8")),
    )
    await consumer.start()

    # Kafka 메시지 소비
    async def consume_messages():
        try:
            async for msg in consumer:
                message_data = msg.value
                await manager.send_message(
                    f"{message_data['sender']}: {message_data['message']}",
                    user1, user2
                )
                save_message_to_mongo(message_data, KAFKA_TOPIC, msg.timestamp, msg.offset)
        finally:
            await consumer.stop()

    # WebSocket 메시지 송신
    async def produce_messages():
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
                    print(f"WebSocket disconnected for {user1}-{user2}.")
                    break
        finally:
            await manager.disconnect(websocket, user1, user2)

    try:
        # 메시지 소비 및 송신 병렬 처리
        await asyncio.gather(consume_messages(), produce_messages())
    except Exception as e:
        print(f"WebSocket handling error: {e}")
    finally:
        await websocket.close()

@app.get("/api/chat_rooms/{user_id}")
def get_chat_rooms(user_id: str):
    try:
        collections = db.list_collection_names()
        user_related_topics = [
            topic for topic in collections if user_id in topic
        ]
        chat_rooms = []
        for topic in user_related_topics:
            users = topic.split("-")
            partner_id = users[0] if users[1] == user_id else users[1]

            # MongoDB에서 닉네임 조회
            try:
                print(f"Attempting to fetch nickname for partner_id: {partner_id}, type: {type(partner_id)}")
                partner_info = client["cinetalk"]["user"].find_one({"_id": ObjectId(partner_id)})
                print(f"Fetched partner_info: {partner_info}")
                partner_nickname = partner_info.get("nickname", "Unknown") if partner_info else "Unknown"
            except Exception as e:
                partner_nickname = "Unknown"
                print(f"Error retrieving user nickname for ID {partner_id}: {e}")

            chat_rooms.append({
                "topic": topic,
                "partner_id": partner_id,
                "partner_nickname": partner_nickname,
            })
        return {"status": "success", "data": chat_rooms}
    except Exception as e:
        return {"status": "error", "message": str(e)}


# FastAPI 이벤트 처리
@app.on_event("startup")
async def startup_event():
    """Kafka Producer 시작"""
    await producer.start()

@app.on_event("shutdown")
async def shutdown_event():
    """Kafka Producer 종료"""
    await producer.stop()
