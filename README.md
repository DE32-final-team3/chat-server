# 🎞️ CINEMATE Chatting server
Kafka and WebSocket Server Running in a Docker Container

## Source code Structure
```
├── docker-compose-kafka.yml     # Kafka/Kafka-ui/zookeeper docker compose file
├── docker-compose-socket.yml    # Websocket docker compose file
└── websocket
    ├── Dockerfile
    ├── main.py                  # WebSocket settings for real-time chat/notifications with Kafka and MongoDB
    ├── requirements.txt
    └── websocket_manager.py     # ChatManager class for WebSocket

1 directory, 7 files
```

## Usage
1. To start Kafka and related services:
```
$ docker-compose -f docker-compose-kafka.yml up -d
```
2. To start the WebSocket server:
```
$ docker-compose -f docker-compose-socket.yml up -d
```

## Access Ports
```
- Kafka UI : <user-IP>:8080
- Websocket api Docs : <user-IP>:8000/docs
```

## Websocket API endpoints
- `GET /api/chat_rooms/{user_id}`

  Retrieves a list of chat rooms associated with the given user, including the partner's ID and nickname.
- `GET /api/chat_rooms/{user_id}/recent_messages`

  Fetches recent messages from each chat room linked to the user, including the latest message details.
- `GET /api/chat_rooms/{user_id}/unread`

  Returns the unread message count and the latest message for each chat room associated with the user.
- `POST /api/chat_rooms/update_offsets`

  Updates the last read offset for a chat room and calculates the remaining unread message count.
