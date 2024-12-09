from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from aiokafka import AIOKafkaProducer, AIOKafkaConsumer
from websocket_manager import ChatManager
import asyncio
import json

app = FastAPI()
KAFKA_BROKER_URL = "kafka:9092"

manager = ChatManager()

# WebSocket 엔드포인트
@app.websocket("/ws/{user1}/{user2}")
async def websocket_endpoint(websocket: WebSocket, user1: str, user2: str):
    # 유저 쌍에 대한 Kafka topic 설정
    KAFKA_TOPIC = f"{min(user1, user2)}-{max(user1, user2)}"

    # WebSocket 연결 처리
    await manager.connect(websocket, user1, user2)
    try:
        while True:
            # WebSocket으로부터 메시지 수신
            data = await websocket.receive_text()

            # Kafka로 메시지 전송
            await producer.send_and_wait(KAFKA_TOPIC, json.dumps({"message": data}).encode("utf-8"))
    except WebSocketDisconnect:
        # WebSocket 연결 끊김 처리
        await manager.disconnect(websocket, user1, user2)

# Kafka Consumer 메시지 처리
async def consume_kafka_messages():
    # 모든 활성화된 토픽을 위한 Consumer 생성
    for room_name in manager.active_connections.keys():
        consumer = AIOKafkaConsumer(
            room_name,  # 동적으로 토픽 사용
            bootstrap_servers=KAFKA_BROKER_URL,
            group_id="websocket_group",
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        )
        await consumer.start()
        try:
            async for msg in consumer:
                message = msg.value["message"]
                # 해당 WebSocket 연결로 메시지 전송
                await manager.send_message(message, *msg.topic.split('-'))
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
    # Kafka Consumer를 비동기적으로 실행
    asyncio.create_task(consume_kafka_messages())

@app.on_event("shutdown")
async def shutdown_event():
    await producer.stop()
