import asyncio
from fastapi import WebSocket
from typing import Dict, List, Optional

class ChatManager:
    def __init__(self):
        # {room_name: List[WebSocket]} 형태로 유저 쌍별 채팅방 관리
        self.active_connections: Dict[str, List[WebSocket]] = {}

    async def connect(self, websocket: WebSocket, sender: str, receiver: Optional[str] = None):
        """
        사용자 1명만 받을 경우: notify 토픽 생성.
        사용자 2명을 받을 경우: 유저 쌍별 채팅방 생성.
        """
        if receiver:
            # 2명의 사용자로 채팅방 생성
            room_name = f"{min(sender, receiver)}-{max(sender, receiver)}"
            if room_name not in self.active_connections:
                self.active_connections[room_name] = []
            if websocket not in self.active_connections[room_name]:
                self.active_connections[room_name].append(websocket)
        else:
            # 1명의 사용자로 notify 토픽 생성
            notify_topic = f"{sender}_notify"
            if notify_topic not in self.active_connections:
                self.active_connections[notify_topic] = []
            if websocket not in self.active_connections[notify_topic]:
                self.active_connections[notify_topic].append(websocket)
        # WebSocket 연결 수락
        await websocket.accept()

    async def disconnect(self, websocket: WebSocket, sender: str, receiver: Optional[str] = None):
        """
        사용자 1명만 받을 경우: notify 토픽 연결 해제.
        사용자 2명을 받을 경우: 채팅방 연결 해제.
        """
        if receiver:
            # 2명의 사용자로 채팅방 처리
            room_name = f"{min(sender, receiver)}-{max(sender, receiver)}"
            if room_name in self.active_connections:
                if websocket in self.active_connections[room_name]:
                    self.active_connections[room_name].remove(websocket)
                if not self.active_connections[room_name]:
                    del self.active_connections[room_name]
        else:
            # 1명의 사용자로 notify 토픽 처리
            notify_topic = f"{sender}_notify"
            if notify_topic in self.active_connections:
                if websocket in self.active_connections[notify_topic]:
                    self.active_connections[notify_topic].remove(websocket)
                if not self.active_connections[notify_topic]:
                    del self.active_connections[notify_topic]

    # 특정 방의 모든 연결로 메시지 브로드캐스트
    async def send_message(self, message: str, sender: str, receiver: Optional[str] = None):
        """
        사용자 2명일 경우:
            - 채팅방에 메시지 전송
            - receiver의 notify 토픽으로 알림 메시지 전송
        사용자 1명일 경우:
            - 메시지는 보내지 않고 받기만 함
        """
        if receiver:
            # 채팅방 메시지 전송
            room_name = f"{min(sender, receiver)}-{max(sender, receiver)}"
            if room_name in self.active_connections:
                await asyncio.gather(
                    *[self._safe_send(connection, message) for connection in self.active_connections[room_name]]
                )
            
            # receiver의 notify 토픽으로 알림 메시지 전송
            notify_topic = f"{receiver}_notify"
            if notify_topic in self.active_connections:
                await asyncio.gather(
                    *[self._safe_send(connection, f"[Notify-{receiver}]: {message}") for connection in self.active_connections[notify_topic]]
                )
        else:
            # 사용자 1명일 경우 메시지를 보내지 않음
            print(f"No action taken for single user: {sender}")

    async def _safe_send(self, connection: WebSocket, message: str):
        try:
            await connection.send_text(message)
        except Exception as e:
            print(f"Error sending message to {connection.client}: {e}")
