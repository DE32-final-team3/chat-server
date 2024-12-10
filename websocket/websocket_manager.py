from fastapi import WebSocket
from typing import Dict, List

class ChatManager:
    def __init__(self):
        # {room_name: List[WebSocket]} 형태로 유저 쌍별 채팅방 관리
        self.active_connections: Dict[str, List[WebSocket]] = {}

    async def connect(self, websocket: WebSocket, user1: str, user2: str):
        room_name = f"{min(user1, user2)}-{max(user1, user2)}"  # 유저 쌍을 기준으로 방 이름 생성
        if room_name not in self.active_connections:
            self.active_connections[room_name] = []
        self.active_connections[room_name].append(websocket)
        await websocket.accept()

    async def disconnect(self, websocket: WebSocket, user1: str, user2: str):
        room_name = f"{min(user1, user2)}-{max(user1, user2)}"
        self.active_connections[room_name].remove(websocket)
        
        if not self.active_connections[room_name]:
            del self.active_connections[room_name]

    async def send_message(self, message: str, user1: str, user2: str):
        room_name = f"{min(user1, user2)}-{max(user1, user2)}"
        if room_name in self.active_connections:
            for connection in self.active_connections[room_name]:
                await connection.send_text(message)
