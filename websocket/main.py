from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from aiokafka import AIOKafkaProducer, AIOKafkaConsumer
from websocket_manager import ChatManager
import asyncio
import json

app = FastAPI()

# Kafka 설정
KAFKA_BROKER_URL = "kafka:9092"
KAFKA_TOPIC = "chat-1"

manager = ChatManager()


# producer = AIOKafkaProducer(bootstrap_servers=KAFKA_BROKER_URL)

# manager = ChatManager()

# # Kafka Producer 및 Consumer 초기화
# @app.on_event("startup")
# async def startup_event():
#     # Kafka Producer 시작
#     await producer.start()
#     #asyncio.create_task(consume_kafka_messages())

# @app.on_event("shutdown")
# async def shutdown_event():
#     # Kafka Producer 종료
#     await producer.stop()

# # WebSocket 엔드포인트
# @app.websocket("/ws/{user1}/{user2}")
# async def websocket_endpoint(websocket: WebSocket, user1: str, user2:str):
#     room_name = f"{min(user1, user2)}-{max(user1, user2)}"

#     await manager.connect(websocket, user1, user2)

#     consumer = AIOKafkaConsumer(
#         room_name,
#         bootstrap_server=KAFKA_BROKER_URL,
#         group_id=f"{room_name}_group",
#         auto_offset_reset="earliest"
#     )
#     await consumer.start()

#     try:
#         async def consume_messages():
#             async for message in consumer:
#                 await websocket.send_text(message.value.decode("utf-8"))

#         consumer_task = asyncio.create_task(consume_messages())

#         while True:
#             data = await websocket.receive_text()

#             await producer.send_and_wait(room_name, data.encode("utf-8"))

#     except WebSocketDisconnect:
#         await manager.disconnect(websocket, user1, user2)
#     finally:
#         # Kafka Consumer 정리
#         await consumer.stop()
#         consumer_task.cancel()

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
