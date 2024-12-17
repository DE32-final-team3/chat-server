import asyncio
from fastapi import WebSocket
from typing import Dict, List

class ChatManager:
    def __init__(self):
        # {room_name: List[WebSocket]} 형태로 유저 쌍별 채팅방 관리
        self.active_connections: Dict[str, List[WebSocket]] = {}

    async def connect(self, websocket: WebSocket, user1: str, user2: str):
        # 유저 ID를 기준으로 정렬하여 고유한 방 이름 생성
        room_name = f"{min(user1, user2)}-{max(user1, user2)}"
        # 방이 없으면 새로 생성
        if room_name not in self.active_connections:
            self.active_connections[room_name] = []
        # 중복된 WebSocket 객체를 추가하지 않도록 확인
        if websocket not in self.active_connections[room_name]:
            self.active_connections[room_name].append(websocket)
        # WebSocket 연결 수락
        await websocket.accept()

    async def disconnect(self, websocket: WebSocket, user1: str, user2: str):
        room_name = f"{min(user1, user2)}-{max(user1, user2)}"
        if room_name in self.active_connections:
            # WebSocket이 방에 존재하면 제거
            if websocket in self.active_connections[room_name]:
                self.active_connections[room_name].remove(websocket)
            # 방이 비어있으면 방 삭제
            if not self.active_connections[room_name]:
                del self.active_connections[room_name]

    # 특정 방의 모든 연결로 메시지 브로드캐스트
    async def send_message(self, message: str, user1: str, user2: str):
        room_name = f"{min(user1, user2)}-{max(user1, user2)}"
        if room_name in self.active_connections:
            await asyncio.gather(
                *[self._safe_send(connection, message) for connection in self.active_connections[room_name]]
            )
            # for connection in self.active_connections[room_name]:
            #     await connection.send_text(message)

    async def _safe_send(self, connection: WebSocket, message: str):
        try:
            await connection.send_text(message)
        except Exception as e:
            print(f"Error sending message to {connection.client}: {e}")