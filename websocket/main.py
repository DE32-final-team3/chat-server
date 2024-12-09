from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException
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

# 특정 Kafka 토픽으로 메시지 전송
@app.post("/send/{topic}")
async def send_message(topic: str, message: str):
    try:
        # Kafka로 메시지 전송
        #message_bytes = json.dumps({"message": message}).encode("utf-8")
        await producer.send_and_wait(topic, message)
        return {"status": "Message sent successfully", "topic": topic, "message": message}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# 특정 Kafka 토픽에서 메시지 수신
@app.get("/receive/{topic}")
async def receive_messages(topic: str):
    messages = []
    consumer = AIOKafkaConsumer(
        topic,
        bootstrap_servers=KAFKA_BROKER_URL,
        group_id="api_router_group",
        value_deserializer=lambda v: v.decode("utf-8"),
    )
    await consumer.start()
    try:
        # 메시지 5개만 가져오기
        async for msg in consumer:
            messages.append(msg.value)
            if len(messages) >= 5:  # 수신 메시지 개수 제한
                break
        return {"status": "Messages received", "topic": topic, "messages": messages}
    finally:
        await consumer.stop()

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
