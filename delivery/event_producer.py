import json
from datetime import datetime, timezone
from kafka import KafkaProducer
from kafka.errors import KafkaError
import sys
import asyncio

from cache_managers.notification_cache_manager import NotificationCacheManager
from cache_managers.cache_message import MessageCacheManager
from ws_manager import manager

notification_cache_manager = NotificationCacheManager()
message_cache_manager = MessageCacheManager()

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    request_timeout_ms=10000,
    api_version=(0, 11, 5)
)

# Create locks for each await operation
message_lock = asyncio.Lock()
notification_lock = asyncio.Lock()
summary_lock = asyncio.Lock()
websocket_lock = asyncio.Lock()
kafka_lock = asyncio.Lock()

async def produce_ack_event(user_producer_id: str, user_consumer_id: str, chat_id: str):
    """
    Handles an "ack_chat" event with locks.
    """
    global manager, producer, message_cache_manager, notification_cache_manager # Ensure access to globals
    print(f"\n--- [ACK Step 1] Starting produce_ack_event for chat {chat_id}, user {user_producer_id}")

    try:
        # 1. Mark all messages in the chat as read.
        print(f"--- [ACK Step 1a] Updating messages to read for chat {chat_id}")
        async with message_lock:
            await message_cache_manager.update_message_from_cache(chat_id, user_producer_id, is_read=True, is_delivered=True)
        print(f"--- [ACK Step 1b] Messages updated for chat {chat_id}")

        # 2. Delete all notifications for this chat for the sender.
        print(f"--- [ACK Step 2a] Deleting notifications for user {user_producer_id}, chat {chat_id}")
        async with notification_lock:
            await notification_cache_manager.delete_notifications_by_chat_id(user_producer_id, chat_id)
        print(f"--- [ACK Step 2b] Notifications deleted for user {user_producer_id}, chat {chat_id}")

        # 3. Update the chat summary for the user.
        print(f"--- [ACK Step 3a] Updating summary for user {user_producer_id}")
        async with summary_lock:
            updated_summary = await notification_cache_manager.update_summary(user_producer_id)
        print(f"--- [ACK Step 3b] Summary updated for user {user_producer_id}")

        print("\n\n !!!!!!!!!!!!!!! UPDATED SUMMARY !!!!!!!!!!!!!!!!!!!!!! : ", updated_summary, "\n\n")
        # 4. Prepare the ack event payload.
        print(f"--- [ACK Step 4a] Preparing ack_event payload")
        ack_event = {
            "event_type": "ack_chat",
            "data": {
                "chat_id": chat_id,
                "timestamp": datetime.now(timezone.utc).isoformat(), # Using timezone aware now
                "data": updated_summary
            }
        }
        print(f"--- [ACK Step 4b] ack_event payload prepared")

        # 6. Send the updated summary immediately to the user's (producer) WebSocket.
        websocket_payload = {
            "event_type": "chats_state_update_on_ack_chat",
            "data": updated_summary
        }

        kafka_topic = f'user-{user_consumer_id}-notifications' # Usually ack goes to the other party
        print(f"--- [ACK Step 5a] Publishing ack_event to Kafka topic {kafka_topic}")
        async with kafka_lock:
            producer.send(kafka_topic, ack_event)
            producer.flush() # Explicitly send messages and wait for delivery.
        print(f"--- [ACK Step 5b] ack_event published to Kafka topic {kafka_topic}")

        print(f"--- [ACK Step 6a] Sending WebSocket update to user {user_producer_id}")
        async with websocket_lock:
            await manager.send_personal_message(user_producer_id, json.dumps(websocket_payload))
        print(f"--- [ACK Step 6b] WebSocket update sent to user {user_producer_id}")

        # 5. Publish the ack event to the Kafka topic for the user (consumer who sent the ack).


        print(f"--- [ACK Step 7] Exiting produce_ack_event for chat {chat_id}, user {user_producer_id}")
        return ack_event

    except Exception as e:
        print(f"--- [ACK ERROR Step] Failed during ack_event processing: {e}")
        return None  # Or handle the error differently

# --- Updated Function with Prints ---
async def produce_message_event(
    user_producer_id: str,
    user_consumer_id: str,
    chat_id: str,
    text: dict | str # Content of the message (can be text or structured data)
):
    """
    Handles a new "message" event with print statements for each step.
    """
    global producer, message_cache_manager, notification_cache_manager, manager # Ensure access to globals
    print(f"\n--- [MSG Step 0] Starting produce_message_event from {user_producer_id} to {user_consumer_id} in chat {chat_id}")

    try:
        # Generate a UTC timestamp
        timestamp_now = datetime.now(timezone.utc).isoformat()
        print(f"--- [MSG Step 0a] Timestamp generated: {timestamp_now}")

        # 1. Store the new message in the cache.
        message_data = {
            "sender": user_producer_id,
            "chat": chat_id,
            "text": text,
            "timestamp": timestamp_now
        }
        print(f"--- [MSG Step 1a] Preparing message data for cache: {message_data}")
        print(f"--- [MSG Step 1b] Calling add_message_to_cache for chat {chat_id}")
        async with message_lock:
            message_data_with_id, chat_participants = await message_cache_manager.add_message_to_cache(
                chat_id=chat_id,
                message_data=message_data
            )
        if message_data_with_id and "_id" in message_data_with_id:
             print(f"--- [MSG Step 1c] Message stored in cache. ID: {message_data_with_id['_id']}")
        else:
             print(f"--- [MSG WARNING Step 1c] Message stored but ID might be missing in return value.")

        # Ensure message_data_with_id has a valid _id before proceeding
        if not message_data_with_id or "_id" not in message_data_with_id:
             print(f"--- [MSG ERROR Step 1d] Cannot proceed without valid message ID after storage attempt. Exiting.")
             return None

        # 2. Add a notification for the recipient.
        notification_payload = {
            "event_type": "message",
            "message_id": message_data_with_id["_id"], # Use the ID returned from cache
            "chat_id": chat_id,
            "sender_id": user_producer_id,
            "message_text": text, # Consider truncating long messages if needed
            "timestamp": timestamp_now,
            "is_read": False
        }
        print(f"--- [MSG Step 2a] Preparing notification data for recipient {user_consumer_id}: {notification_payload}")
        print(f"--- [MSG Step 2b] Calling add_notification for user {user_consumer_id}")
        async with notification_lock:
            await notification_cache_manager.add_notification(
                user_id=user_consumer_id,
                notification_data=notification_payload
            )
        print(f"--- [MSG Step 2c] Notification added for user {user_consumer_id}")

        # 6. Update the chat summary for the SENDER.
        print(f"--- [MSG Step 6a] Updating summary for sender {user_producer_id}")
        async with summary_lock:
            updated_summary_sender = await notification_cache_manager.update_summary(user_producer_id)
        print(f"--- [MSG Step 6b] Summary updated for sender {user_producer_id}")

        # 7. Retrieve the updated full list of messages for this chat.
        print(f"--- [MSG Step 7a] Retrieving messages from cache for chat {chat_id}")
        async with message_lock:
            chat_messages = await message_cache_manager.get_messages_from_cache(chat_id)
        if chat_messages is None:
            print(f"--- [MSG WARNING Step 7b] get_messages_from_cache returned None for chat {chat_id}. Using empty list.")
            chat_messages = []
        else:
            print(f"--- [MSG Step 7b] Retrieved {len(chat_messages)} messages for chat {chat_id}")

        # 8. Send the updated chat state (summary + messages) back to the SENDER's WebSocket.
        print(f"--- [MSG Step 8a] Preparing WebSocket payload for sender {user_producer_id}")
        sender_update_payload = {
            "event_type": "chat_state_update_on_send",
            "data": {
                "chat_id": chat_id,
                "summary": updated_summary_sender,
                # "messages": chat_messages
            }
        }
        print(f"--- [MSG Step 8b] WebSocket payload prepared for sender {user_producer_id}")
        print(f"--- [MSG Step 8c] Sending WebSocket update to sender {user_producer_id}")
        async with websocket_lock:
            await manager.send_personal_message(user_producer_id, json.dumps(sender_update_payload))
        print(f"--- [MSG Step 8d] WebSocket update sent to sender {user_producer_id}")

        # 4. Prepare the message event payload for Kafka.
        print(f"--- [MSG Step 4a] Preparing Kafka message_event payload")
        message_event = {
            "event_type": "message",
            "chat_id": chat_id,
            "message_id": message_data_with_id["_id"],
            "sender_id": user_producer_id,
            "receiver_id": user_consumer_id,
            "message_text": text,
            "timestamp": timestamp_now,
            # "summary": updated_summary_recipient # Removed as per Step 3 skip
            }
        print(f"--- [MSG Step 4b] Kafka message_event payload prepared: {message_event}")

        # 5. Publish the message event to the recipient's Kafka topic.
        kafka_topic = f'user-{user_consumer_id}-notifications'
        print(f"--- [MSG Step 5a] Publishing message_event to Kafka topic {kafka_topic}")
        async with kafka_lock:
            producer.send(kafka_topic, message_event)
            producer.flush()
        print(f"--- [MSG Step 5b] message_event published to Kafka topic {kafka_topic}")

        # Return the original Kafka event payload (intended for the recipient)
        print(f"--- [MSG Step 9] Exiting produce_message_event. Returning Kafka payload.")
        return message_event

    except Exception as e:
        print(f"--- [MSG ERROR Step] Failed during message_event processing: {e}")
        return None