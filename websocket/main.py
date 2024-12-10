from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException
from aiokafka import AIOKafkaProducer, AIOKafkaConsumer
from websocket_manager import ChatManager
import asyncio
import json

app = FastAPI()
KAFKA_BROKER_URL = "kafka:9092"

manager = ChatManager()

# Producer 인스턴스 초기화
producer = AIOKafkaProducer(
    bootstrap_servers=KAFKA_BROKER_URL,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

# WebSocket 엔드포인트
@app.websocket("/ws/{user1}/{user2}")
async def websocket_endpoint(websocket: WebSocket, user1: str, user2: str):
    # 유저 쌍에 대한 Kafka topic 설정
    KAFKA_TOPIC = f"{min(user1, user2)}-{max(user1, user2)}"
    print(f"topic {KAFKA_TOPIC}")

    # WebSocket 연결 처리
    await manager.connect(websocket, user1, user2)
    try:
        while True:
            print("receive_text")
            # WebSocket으로부터 메시지 수신
            data = await websocket.receive_text()
            print(f"data {data}")

            # Kafka로 메시지 전송
            print("send_and_wait")
            await producer.send_and_wait(KAFKA_TOPIC, data)
    except WebSocketDisconnect:
        # WebSocket 연결 끊김 처리
        await manager.disconnect(websocket, user1, user2)

# 특정 Kafka 토픽으로 메시지 전송
@app.post("/send/{topic}")
async def send_message(topic: str, message: str):
    print("send endpoint")
    try:
        # Kafka로 메시지 전송
        #message_bytes = json.dumps({"message": message}).encode("utf-8")
        await producer.send_and_wait(topic, json.dumps({"message": message}))
        return {"status": "Message sent successfully", "topic": topic, "message": message}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# 특정 Kafka 토픽에서 메시지 수신
@app.get("/receive/{topic}")
async def receive_messages(topic: str):
    print("receive endpoint")
    messages = []
    consumer = AIOKafkaConsumer(
        topic,
        bootstrap_servers=KAFKA_BROKER_URL,
        group_id="websocket_group",
        auto_offset_reset="earliest",
        value_deserializer=lambda x: json.loads(x.decode("utf-8")),
        #consumer_timeout_ms=10000,
    )
    await consumer.start()

    try:
        one = await consumer.getone()
        msg = one.value
        #async for msg in consumer:
        #    print(msg.value)
        #    messages.append(msg.value)
            #if len(messages) >= 5:  # 수신 메시지 개수 제한
            #    break
        return {"status": "Messages received", "topic": topic, "messages": msg}
    finally:
        await consumer.stop()



# Kafka Consumer 메시지 처리
async def consume_kafka_messages():
    consumer = AIOKafkaConsumer(
        *manager.active_connections.keys(),  # 모든 활성 토픽 구독
        bootstrap_servers=KAFKA_BROKER_URL,
        group_id="websocket_group",
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    )
    await consumer.start()
    try:
        async for msg in consumer:
            message = msg.value["message"]
            # WebSocket으로 메시지 전송
            await manager.send_message(message, *msg.topic.split('-'))
    finally:
        await consumer.stop()


consumers = []
# Kafka Producer 및 Consumer 초기화
@app.on_event("startup")
async def startup_event():
    print("startup")
    await producer.start()
    # Kafka Consumer를 비동기적으로 실행
    for room_name in manager.active_connections.keys():
        consumer = AIOKafkaConsumer(
            room_name,
            bootstrap_servers=KAFKA_BROKER_URL,
            group_ip="websocket_group",
            value_deserializer=lambda v:json.loads(v.decode("utf-8")),
        )
        await consumer.start()
        consumers.append(consumer)
    asyncio.create_task(consume_kafka_messages())

@app.on_event("shutdown") 
async def shutdown_event():
    await producer.stop()
    for consumer in consumers:
        await consumer.stop()
