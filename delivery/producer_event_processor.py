import json
from datetime import datetime, timezone
import asyncio


class NotificationEventProcessorPRODUCER:
    """
    EventProcessorPRODUCER is responsible for processing producer events for Kafka. It provides two methods:

      - process_ack_event: Processes "ack_chat" events.
      - process_message_event: Processes new "message" events.

    This class delegates responsibilities to existing cache managers, Kafka producer, and WebSocket manager.

    Dependencies (provided via constructor):
      - message_manager: Instance of MessageCacheManager for message cache operations.
      - notification_cache_manager: Instance of NotificationCacheManager for notification CRUD operations.
      - notification_analytics_manager: Instance of NotificationAnalyticsManager for updating and retrieving summaries.
      - kafka_producer: A KafkaProducer instance used for publishing events.
      - websocket_manager: An instance of a WebSocket manager with a method send_personal_message(user_id, message).
      - locks: A dict of asyncio.Lock objects. Expected keys include:
            "message_lock", "notification_lock", "summary_lock", "websocket_lock", "kafka_lock".
    """

    def __init__(self, message_manager, notification_cache_manager, notification_analytics_manager,
                 kafka_producer, websocket_manager, locks=None):
        if locks is None:
            locks = {
                "message_lock": asyncio.Lock(),
                "notification_lock": asyncio.Lock(),
                "summary_lock": asyncio.Lock(),
                "websocket_lock": asyncio.Lock(),
                "kafka_lock": asyncio.Lock()
            }
        self.message = message_manager
        self.notification_cache = notification_cache_manager
        self.notification_analytics = notification_analytics_manager
        self.producer = kafka_producer
        self.websocket_manager = websocket_manager
           
        self.message_lock = locks.get("message_lock")
        self.notification_lock = locks.get("notification_lock")
        self.summary_lock = locks.get("summary_lock")
        self.websocket_lock = locks.get("websocket_lock")
        self.kafka_lock = locks.get("kafka_lock")

    async def produce_ack_event(self, user_producer_id: str, user_consumer_id: str, chat_id: str) -> dict:
        """
        produces an 'ack_chat' event for a specific chat.

        Steps:
          1. Marks all messages in the specified chat as read and delivered for the producer.
          2. Deletes all notifications associated with that chat for the producer.
          3. Updates the analytical summary for the producer.
          4. Constructs an 'ack_chat' event payload with the updated summary.
          5. Publishes the ack event to Kafka on the recipient's notifications topic.
          6. Sends a real-time WebSocket update to the producer with the updated summary.

        Parameters:
          - user_producer_id (str): The ID of the user initiating the acknowledgment.
          - user_consumer_id (str): The target user (recipient) of the Kafka event.
          - chat_id (str): The unique identifier of the chat.

        Returns:
          dict: The acknowledgment event payload.
        """
        # Step 1: Update message statuses in the cache.
        async with self.message_lock:
            await self.message.update_message(chat_id, user_producer_id, is_read=True, is_delivered=True)

        # Step 2: Delete notifications for this chat for the producer.
        async with self.notification_lock:
            await self.notification_cache.delete_notifications_by_chat_id(user_producer_id, chat_id)

        # Step 3: Update the producer's analytical summary.
        async with self.summary_lock:
            updated_summary = await self.notification_analytics.update_summary(user_producer_id)

        # Step 4: Construct the acknowledgment event payload.
        ack_event = {
            "event_type": "ack_chat",
            "data": {
                "chat_id": chat_id,
                "sender_id": user_producer_id,
                "timestamp": datetime.now(timezone.utc).isoformat(),
            }
        }

        # Step 5: Publish the ack event to Kafka under the recipient's topic.
        kafka_topic = f'user-{user_consumer_id}-notifications'
        async with self.kafka_lock:
            self.producer.send(kafka_topic, ack_event)
            self.producer.flush()

        # Step 6: Send a WebSocket update to the producer with the new summary.
        websocket_payload = {
            "event_type": "chats_state_update_on_ack_chat",
            "data": {
                "chat_id": chat_id,
                "summary": updated_summary
                }
        }
        async with self.websocket_lock:
            await self.websocket_manager.send_personal_message(user_producer_id, json.dumps(websocket_payload))

        return ack_event

    async def produce_message_event(self, user_producer_id: str, user_consumer_id: str,
                                    chat_id: str, text: str) -> dict:
        """
        Produces a new 'message' event.

        Steps:
          1. Generate a current UTC timestamp.
          2. Store the new message in the message cache and retrieve the message ID.
          3. Create a notification for the receiver using the notification cache.
          4. Update the analytical summary for the sender.
          5. Retrieve the updated messages for the chat from the cache.
          6. Send a WebSocket update to the sender with the updated summary.
          7. Construct a Kafka event payload with message details.
          8. Publish the message event to Kafka on the recipient's notifications topic.

        Parameters:
          - user_producer_id (str): The sender's user ID.
          - user_consumer_id (str): The receiver's user ID.
          - chat_id (str): The unique identifier of the chat.
          - text (str): The content of the message.

        Returns:
          dict: The message event payload published to Kafka.
        """
        # Step 1: Generate current UTC timestamp.
        timestamp = datetime.now(timezone.utc).isoformat()

        # Step 2: Store the new message in the cache.
        message_data = {
            "sender": user_producer_id,
            "chat": chat_id,
            "text": text,
            "timestamp": timestamp
        }
        async with self.message_lock:
            message_data_with_id, chat_participants = await self.message.add_message_to_cache(chat_id, message_data)

        if not message_data_with_id or "_id" not in message_data_with_id:
            # If storing the message failed (or no ID was returned), exit early.
            return None

        # Step 3: Create a notification for the receiver.
        notification_payload = {
            "type": "message",
            "body": {
            "message_id": message_data_with_id["_id"],
            "chat_id": chat_id,
            "sender_id": user_producer_id,
            "message_text": text,
            "timestamp": timestamp,
            }
        }

        """
            message_id: str
            chat_id: str
            sender_id: str
            message_text: str
            timestamp: str
        """
        async with self.notification_lock:
            await self.notification_cache.add_notification(user_id=user_consumer_id,
                                                           notification_data=notification_payload)

        # Step 4: Update the analytical summary for the sender.
        async with self.summary_lock:
            updated_summary_sender = await self.notification_analytics.update_summary(user_producer_id)

        # Step 5: Retrieve updated messages for the chat.
        async with self.message_lock:
            chat_messages = await self.message.get_messages(chat_id)
        if chat_messages is None:
            chat_messages = []  # Fallback to empty list if nothing is returned.

        # Step 6: Send a WebSocket update to the sender with the new summary.
        sender_update_payload = {
            "event_type": "chat_state_update_on_send",
            "data": {
                "chat_id": chat_id,
                "summary": updated_summary_sender
            }
        }
        async with self.websocket_lock:
            await self.websocket_manager.send_personal_message(user_producer_id, json.dumps(sender_update_payload))

        # Step 7: Prepare the Kafka event payload for the new message.
        message_event = {
            "event_type": "message",
            "body": {
            "message_id": message_data_with_id["_id"],
            "chat_id": chat_id,
            "sender_id": user_producer_id,
            "message_text": text,
            "timestamp": timestamp,
            }
        }

        # Step 8: Publish the message event to Kafka for the recipient.
        kafka_topic = f'user-{user_consumer_id}-notifications'
        async with self.kafka_lock:
            self.producer.send(kafka_topic, message_event)
            self.producer.flush()

        return message_event
