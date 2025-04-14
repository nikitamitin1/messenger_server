from typing import List, Dict
from fastapi import WebSocket
import json


class WebSocketManager:
    def __init__(self):
        # Store active WebSocket connections by user_id
        self.active_connections: Dict[str, List[WebSocket]] = {}

    async def connect(self, websocket: WebSocket, user_id: str):
        """
        Accept a new WebSocket connection and add it to the list of active connections for the user.
        """
        # Accept the WebSocket connection
        await websocket.accept()

        # Add the WebSocket connection to the dictionary for the given user_id
        if user_id not in self.active_connections:
            self.active_connections[user_id] = []
        self.active_connections[user_id].append(websocket)

    def disconnect(self, websocket: WebSocket, user_id: str):
        """
        Remove a WebSocket connection from the list of active connections for the user.
        """
        if user_id in self.active_connections:
            self.active_connections[user_id].remove(websocket)
            # If there are no more connections for the user, remove the user from the active_connections dictionary
            if not self.active_connections[user_id]:
                del self.active_connections[user_id]

    async def send_personal_message(self, receiver_id: str, message: str):
        """
        Send a personal message to a specific WebSocket client by user_id.
        """
        # Check if the user has any active connections
        if receiver_id in self.active_connections:
            # Send the message to each of the user's active connections
            for connection in self.active_connections[receiver_id]:
                await connection.send_text(message)

    async def broadcast(self, message: str):
        """
        Send a broadcast message to all connected WebSocket clients.
        """
        for user_connections in self.active_connections.values():
            for connection in user_connections:
                await connection.send_text(message)


manager = WebSocketManager()