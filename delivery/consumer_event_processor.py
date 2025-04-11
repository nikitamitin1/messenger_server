import json
import asyncio
from datetime import datetime, timezone
from aiokafka import AIOKafkaConsumer

from cache_managers_upd.message_manager import MessageCacheManager
from cache_managers_upd.chat_manager import ChatCacheManager
from cache_managers_upd.notifications_manager.notifications_manager import NotificationAnalyticsManager, NotificationCacheManager

class NotificationEventProcessorCONSUMER:
    """
    NotificationEventConsumer is responsible for consuming Kafka events for a specific user
    and processing them based on event type. It handles both "message" events and "ack_chat" events.

    Dependencies:
      - consumer: An AIOKafkaConsumer instance subscribed to the user's notifications topic.
      - user_id: The user's unique identifier.
      - chat_id (optional): If provided, the consumer filters events for a specific chat.
      - message_cache_manager: Manages message cache operations.
      - notification_cache_manager: Manages notifications in the cache.
      - chat_cache_manager: Retrieves chat information (including the last_message).
      - websocket_manager: Used to send personal WebSocket messages to the user.
      - locks: A dict containing asyncio.Lock objects for critical sections (keys:
                "message_lock", "notification_lock", "summary_lock", "websocket_lock", "kafka_lock").

    This class encapsulates the full event processing loop and delegates processing of different event types
    to dedicated methods.
    """

    def __init__(self, consumer: AIOKafkaConsumer, user_consumer_id: str, chat_id: str,
                 message_cache_manager: MessageCacheManager, notification_cache_manager: NotificationCacheManager, notification_analytics_manager: NotificationAnalyticsManager,
                 chat_cache_manager: ChatCacheManager, websocket_manager, locks: dict = None):
        self.consumer = consumer
        self.user_id = user_consumer_id
        self.chat_id = chat_id
        self.message_manager = message_cache_manager
        self.notification_cache_manager = notification_cache_manager
        self.notification_analytics_manager = notification_analytics_manager
        self.chat_cache_manager = chat_cache_manager
        self.websocket_manager = websocket_manager
        self.locks = locks or {
            "message_lock": asyncio.Lock(),
            "notification_lock": asyncio.Lock(),
            "summary_lock": asyncio.Lock(),
            "websocket_lock": asyncio.Lock(),
            "kafka_lock": asyncio.Lock()
        }
        self.filtered_messages = []

    async def start(self):
        """
        Starts the Kafka consumer, publishes an initial summary via WebSocket, and processes
        incoming Kafka messages in an asynchronous loop.
        """
        # Start Kafka consumer
        await self.consumer.start()
        try:
            # Publish an initial summary to the user channel:
            async with self.locks["summary_lock"]:
                # Initialize and update the user's summary in Redis.
                initial_summary = await self.notification_analytics_manager.compute_summary(self.user_id)
            async with self.locks["websocket_lock"]:
                await self.websocket_manager.send_personal_message(
                    self.user_id,
                    json.dumps({"event_type": "initial_summary", "summary": initial_summary})
                )
            # Main event loop: process each incoming message from Kafka.
            async for msg in self.consumer:
                event = msg.value  # Decoded JSON dictionary.
                event_type = event.get("event_type")
                if event_type == "message":
                    await self.process_message_event(event)
                elif event_type == "ack_chat":
                    await self.process_ack_event(event)
                # More event types can be added here.
        except Exception as e:
            print(f"[CONSUMER ERROR] Error during consuming events: {e}")
        finally:
            # Ensure the consumer is properly closed.
            await self.consumer.stop()
            print(f"[CONSUMER] Kafka consumer stopped for user {self.user_id}")

    async def process_message_event(self, event: dict):
        """
        Processes a "message" event.

        - If a specific chat_id is provided (i.e., self.chat_id is set), it filters events for that chat,
          marks the message as delivered and read, retrieves updated messages, and sends a real-time update via WebSocket.
        - If no chat_id is provided, it processes the event as a general notification update,
          marking the message as delivered and updating the user's summary.
        """
        # If the consumer is bound to a specific chat, process accordingly.
        if self.chat_id:
            if event.get("chat_id") == self.chat_id:
                self.filtered_messages.append(event)
                async with self.locks["message_lock"]:
                    await self.message_manager.update_message(
                        self.chat_id,
                    )
                async with self.locks["message_lock"]:
                    messages_from_chat = await self.message_manager.get_messages(self.chat_id)
                update_payload = {
                    "event_type": "new_message",
                    "data": messages_from_chat
                }
                async with self.locks["websocket_lock"]:
                    await self.websocket_manager.send_personal_message(self.user_id, json.dumps(update_payload))
        else:
            # General processing if no chat-specific filtering is required.
            self.filtered_messages.append(event)
            async with self.locks["message_lock"]:
                await self.message_manager.update_message(
                    event.get("chat_id"),
                    # event.get("message_id"),
                    update = {"is_delivered":True}
                )
            async with self.locks["summary_lock"]:
                updated_summary = await self.notification_analytics_manager.compute_summary(self.user_id)
            async with self.locks["websocket_lock"]:
                await self.websocket_manager.send_personal_message(
                    self.user_id,
                    json.dumps({"event_type": "chat_state_update_on_new_message", "data": updated_summary})
                )

    async def process_ack_event(self, event: dict):
        """
        Processes an "ack_chat" event.

        Steps:
          1. Updates the user's summary using NotificationCacheManager.
          2. Sends an updated summary via WebSocket.
          3. If a specific chat is involved (self.chat_id matches the event's chat_id), retrieves the messages
             from that chat and sends a detailed update via WebSocket.
        """
        async with self.locks["summary_lock"]:
            updated_summary = await self.notification_analytics_manager.compute_summary(self.user_id)
        async with self.locks["websocket_lock"]:
            await self.websocket_manager.send_personal_message(
                self.user_id,
                json.dumps({"event_type": "chat_state_update_on_ack_chat", "data": updated_summary})
            )
        if self.chat_id and event.get("chat_id") == self.chat_id:
            async with self.locks["message_lock"]:
                messages_from_chat = await self.message_manager.get_messages(self.chat_id)
            async with self.locks["websocket_lock"]:
                await self.websocket_manager.send_personal_message(
                    self.user_id,
                    json.dumps({"event_type": "chat_state_update_on_ack_chat", "data": messages_from_chat})
                )

# Example usage:
# Create an instance of AIOKafkaConsumer, passing necessary parameters.
# consumer = AIOKafkaConsumer('user-<user_id>-notifications', group_id='some_group', bootstrap_servers=['localhost:9092'], value_deserializer=lambda x: json.loads(x.decode('utf-8')))
#
# Then inject all dependencies (cache managers, websocket manager, and locks) into the NotificationEventConsumer:
#
# consumer_instance = NotificationEventConsumer(
#     consumer=consumer,
#     user_id="user123",
#     chat_id="chat456",  # Or None if processing all notifications.
#     message_cache_manager=message_cache_manager,
#     notification_cache_manager=notification_cache_manager,
#     chat_cache_manager=chat_cache_manager,
#     websocket_manager=manager,  # Assuming 'manager' is the WebSocket manager instance.
#     locks={
#         "message_lock": message_lock,
#         "notification_lock": notification_lock,
#         "summary_lock": summary_lock,
#         "websocket_lock": websocket_lock,
#         "kafka_lock": kafka_lock
#     }
# )
#
# Finally, call await consumer_instance.start() within an asyncio loop.
