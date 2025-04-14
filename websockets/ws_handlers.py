import json
import asyncio
from delivery.consumer_event_processor import NotificationEventProcessorCONSUMER
from delivery.producer_event_processor import NotificationEventProcessorPRODUCER

from cache_managers_upd.message_manager import MessageCacheManager
from cache_managers_upd.chat_manager import ChatCacheManager
from cache_managers_upd.notifications_manager.notifications_manager import NotificationAnalyticsManager, NotificationCacheManager

class WebsocketEventHandler:
    """
    WebsocketEventHandler handles incoming WebSocket events for chat operations and delegates
    the business logic to the corresponding event producer methods.

    Currently, it supports the following events:
      - handle_send_message: Processes a send message event by sending the message to all chat participants (other than the sender).
      - handle_open_chat: Processes an open chat event by generating an acknowledgement (ack) event for all other participants.

    Dependencies (injected via the constructor):
      - message_cache_manager: Instance of MessageCacheManager to retrieve and update message data.
      - chat_cache_manager: Instance of ChatCacheManager to retrieve chat information (including participants).
      - event_producer: An object (or module) that provides methods produce_message_event and produce_ack_event for sending events.
      - websocket_manager: An instance of a WebSocket manager with a method send_personal_message(user_id, message)
    """

    def __init__(self, message_cache_manager: MessageCacheManager, chat_cache_manager: ChatCacheManager, event_producer: NotificationEventProcessorPRODUCER, websocket_manager):
        self.message_cache_manager = message_cache_manager
        self.chat_cache_manager = chat_cache_manager
        self.event_producer = event_producer
        self.websocket_manager = websocket_manager

    async def handle_send_message(self, data: dict, user_id: str):
        """
        Handles the "send_message" event received via WebSocket.

        Process:
          1. Parse the provided data to extract sender ID, chat ID, and message text.
          2. Retrieve the list of chat participants using ChatCacheManager.
          3. For each participant (except the sender), produce a message event by calling produce_message_event.

        Parameters:
          - data (dict): Dictionary with keys: "sender", "chat", and "text".
          - user_id (str): The ID of the user who sent the message (also the sender).
        """
        # Extract necessary fields from the received data.
        sender_id = data.get("sender")
        chat_id = data.get("chat")
        text = data.get("text")

        # Retrieve the list of chat participants for the given chat.
        chat_info = await self.chat_cache_manager.get_chat_info(chat_id)
        chat_participants = chat_info.get("participants")
        # Iterate through the participants and produce a message event for every user who is not the sender.
        for participant in chat_participants:
            if participant.get("id") != user_id:
                # Delegate the event production to the event_producer.
                await produce_message_event(
                    user_producer_id=user_id,
                    user_consumer_id=participant.get("id"),
                    chat_id=chat_id,
                    text=text
                )

    async def handle_open_chat(self, user_id: str, chat_id: str):
        """
        Handles the "open_chat" event received via WebSocket.

        Process:
          1. Retrieve the list of chat participants for the specified chat.
          2. For each participant (except the user who opened the chat), produce an acknowledgement event
             by calling produce_ack_event.

        Parameters:
          - user_id (str): The ID of the user who opened the chat.
          - chat_id (str): The unique identifier of the chat that was opened.
        """
        # Retrieve participants of the specified chat.
        chat_participants = await self.chat_cache_manager.get_participants_for_chat(chat_id)
        # For each participant except the user who opened the chat, trigger an ack event.
        for participant in chat_participants:
            if participant.get("id") != user_id:
                await produce_ack_event(
                    user_producer_id=user_id,
                    user_consumer_id=participant.get("id"),
                    chat_id=chat_id
                )
