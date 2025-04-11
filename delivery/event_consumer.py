from kafka import KafkaConsumer
import json
from ws_manager import manager
from aiokafka import AIOKafkaConsumer
from cache_managers.cache_message import MessageCacheManager
from cache_managers.notification_cache_manager import NotificationCacheManager
from .event_producer import produce_ack_event

import json
from datetime import datetime, timezone
from aiokafka import AIOKafkaProducer
import asyncio
import logging

notification_cache_manager = NotificationCacheManager()
message_cache_manager = MessageCacheManager()

# Locks for asynchronous operations
message_lock = asyncio.Lock()
notification_lock = asyncio.Lock()
summary_lock = asyncio.Lock()
websocket_lock = asyncio.Lock()
kafka_lock = asyncio.Lock()

async def process_user_notifications(user_consumer_id, chat_id=None):
    """
    Asynchronously process notifications from Kafka for a specific user, with locks.

    If chat_id is provided:
      - For a "send_message" event: update cache marking the message as delivered and read,
        then send it directly to the WebSocket.

    If chat_id is not provided:
      - For a "send_message" event: update delivered status and store the notification in Redis.

    For an "ack_chat" event:
      - Clear notifications for that chat and update message statuses to read.
    """
    print(f"--- [CONSUMER STEP 1] Starting notification processing for user: {user_consumer_id}, chat_id: {chat_id} ---")
    # Create an asynchronous Kafka consumer for the user's notifications topic
    consumer = AIOKafkaConsumer(
        f'user-{user_consumer_id}-notifications',
        group_id=f'{user_consumer_id}-group',
        bootstrap_servers=['localhost:9092'],
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        auto_offset_reset = "latest"
    )
    print(f"--- [CONSUMER STEP 2] Starting Kafka consumer for user: {user_consumer_id} ---")
    await consumer.start()
    async with summary_lock:
        await notification_cache_manager.default_summary_for_user_chats(user_consumer_id)
        initial_summary = await notification_cache_manager.update_summary(user_consumer_id)
    async with websocket_lock:
        await manager.send_personal_message(user_consumer_id, json.dumps({"event_type": "initial_summary", "summary": initial_summary}))
    print(f"--- [CONSUMER STEP 3] Kafka consumer started for user: {user_consumer_id} ---")
    filtered_messages = []
    try:
        async for message in consumer:
            print(f"--- [CONSUMER STEP 4] Received message from Kafka for user: {user_consumer_id} ---")
            notification_data = message.value
            event_type = notification_data.get('event_type')
            print(f"--- [CONSUMER STEP 5] Processing event type: {event_type} ---")

            if event_type == "message":
                print(f"--- [CONSUMER MESSAGE 6] Processing message event for user: {user_consumer_id} ---")
                if chat_id:
                    print(f"--- [CONSUMER MESSAGE STEP 7] Chat ID specified, filtering by chat ID: {chat_id} ---")
                    # If user is in a specific chat, filter messages by chat_id
                    if notification_data.get('chat_id') == chat_id:
                        print(f"--- [CONSUMER MESSAGE STEP 8] Message matches chat ID: {chat_id} ---")
                        filtered_messages.append(notification_data)
                        # Update cache: mark message as delivered and read
                        # (Assumes update_message_from_cache accepts parameters to update delivery and read status)
                        print(f"--- [CONSUMER MESSAGE 9] Calling produce_ack_event for chat: {chat_id} ---")
                        # async with kafka_lock:
                        #     await produce_ack_event(user_consumer_id, notification_data.get('sender_id'), chat_id)
                        # Send the notification directly to the user via WebSocket
                        print(f"--- [CONSUMER MESSAGE 10] Retrieving messages from chat: {chat_id} ---")
                        async with message_lock:
                            messages_from_chat = message_cache_manager.get_messages_from_cache(chat_id)
                        print(f"--- [CONSUMER MESSAGE 11] Sending new message event via WebSocket for chat: {chat_id} ---")
                        async with websocket_lock:
                            await manager.send_personal_message(user_consumer_id, {'event_type': 'new_message', 'data': {json.dumps(messages_from_chat)}})
                else:
                    print(f"--- [CONSUMER MESSAGE 12] No chat ID specified, user offline from specific chat ---")
                    # If no specific chat is specified, the user is offline from any particular chat.
                    filtered_messages.append(notification_data)
                    # Update cache: mark message as delivered only
                    print(f"--- [CONSUMER MESSAGE 13] Updating message delivery status for message ID: {notification_data.get('message_id')} ---")
                    async with message_lock:
                        await message_cache_manager.update_message_from_cache(
                            notification_data.get('chat_id'), notification_data['message_id'], is_delivered=True
                        )
                    # Store the notification in the user's notifications table in Redis.
                    # await notification_cache_manager.add_notification(user_consumer_id, notification_data)
                    print(f"--- [CONSUMER MESSAGE 14] Updating user summary for user: {user_consumer_id} ---")
                    async with summary_lock:
                        updated_summary = await notification_cache_manager.update_summary(user_consumer_id)
                    async with websocket_lock:
                        await manager.send_personal_message(user_consumer_id, json.dumps({'event_type': 'chat_state_update_on_new_message', 'data': updated_summary}))
                    print(f"--- [CONSUMER MESSAGE 15] Sending test message via websocket for user: {user_consumer_id} ---")
            elif event_type == "ack_chat":
                print(f"--- [CONSUMER ACK CHAT 16] Processing ack_chat event for user: {user_consumer_id} ---")
                # THIS EVENT IS FOR ME, AND I RECIEVE ACK IT MEANS THAT I NEED TO 1) UPDATE MY FRONT END
                # "ack_chat" means the user has opened a specific chat.

                # WE RECIEVE NOTIFICATION OF ACK IT MEANS THAT WE NEED TO REQUEST UPDATED STATE OF CHATS
                print(f"--- [CONSUMER ACK CHAT 17] Updating user summary for user: {user_consumer_id} ---")
                async with summary_lock:
                    updated_summary = await notification_cache_manager.update_summary(user_consumer_id)
                print(f"--- [CONSUMER ACK CHAT 17a] Updated summary: {updated_summary} for user: {user_consumer_id}")
                print(f"--- [CONSUMER ACK CHAT 18] Sending chat state update on ack chat via WebSocket for user: {user_consumer_id} ---")
                async with websocket_lock:
                    await manager.send_personal_message(user_consumer_id, json.dumps({'event_type': 'chat_state_update_on_ack_chat', 'data': updated_summary}))
                if chat_id and chat_id == notification_data.get('chat_id'):
                    print(f"--- [CONSUMER ACK CHAT 19] Chat ID specified and matches, retrieving messages from chat: {chat_id} ---")
                    # IF OUR CONSUMER LISTENS WITH CHAT_ID SPECIFIED IT MEANS THAT WE WANT TO GET INFORMATION FOR THIS PARTICULAR CHAT
                    async with message_lock:
                        messages_from_chat = message_cache_manager.get_messages_from_cache(chat_id)
                    print(f"--- [CONSUMER ACK CHAT 20] Sending chat state update on ack chat with messages via WebSocket for chat: {chat_id} ---")
                    async with websocket_lock:
                        await manager.send_personal_message(user_consumer_id, {'event_type': f'chat_state_update_on_ack_chat', 'data': {json.dumps(messages_from_chat)}})
            print(f"--- [CONSUMER STEP 21] Finished processing message for user: {user_consumer_id} ---")

    finally:
        print(f"--- [CONSUMER STEP 22] Stopping Kafka consumer for user: {user_consumer_id} ---")
        await consumer.stop()
        print(f"--- [CONSUMER STEP 23] Kafka consumer stopped for user: {user_consumer_id} ---")
    print(f"--- [CONSUMER STEP 24] Finished notification processing for user: {user_consumer_id}, chat_id: {chat_id} ---")
    return filtered_messages