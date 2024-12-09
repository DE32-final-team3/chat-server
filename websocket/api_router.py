from fastapi import APIRouter, HTTPException
from aiokafka import AIOKafkaProducer, AIOKafkaConsumer
import asyncio
import json

KAFKA_BROKER_URL = "kafka:9092"

api_router = APIRouter()

# Producer 인스턴스 초기화
producer = AIOKafkaProducer(
    bootstrap_servers=KAFKA_BROKER_URL,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

# 특정 Kafka 토픽으로 메시지 전송
@api_router.post("/send/{topic}")
async def send_message(topic: str, message: str):
    try:
        await producer.send_and_wait(topic, {"message": message})
        return {"status": "Message sent successfully", "topic": topic, "message": message}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# 특정 Kafka 토픽에서 메시지 수신
@api_router.get("/receive/{topic}")
async def receive_messages(topic: str):
    messages = []
    consumer = AIOKafkaConsumer(
        topic,
        bootstrap_servers=KAFKA_BROKER_URL,
        group_id="api_router_group",
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    )
    await consumer.start()
    try:
        # 메시지 5개만 가져오기 (비동기 반복자)
        async for msg in consumer:
            messages.append(msg.value)
            if len(messages) >= 5:  # 수신 메시지 개수 제한
                break
        return {"status": "Messages received", "topic": topic, "messages": messages}
    finally:
        await consumer.stop()
