import redis
import motor.motor_asyncio
import json
from bson import ObjectId
import os
import datetime

class MessageCacheManager:
    def __init__(self, db_url=os.getenv("DB_URL", "mongodb://admin:admin@localhost:27017"), redis_url='redis://localhost:6379'):
        # Connect to MongoDB
        self.client = motor.motor_asyncio.AsyncIOMotorClient(db_url)
        self.db = self.client['whatsapp_clone']

        # Connect to Redis
        self.redis = redis.asyncio.StrictRedis.from_url(redis_url, decode_responses=True)

    async def load_messages_to_cache(self, user_id):
        """
        Load all messages for the chats the user is in and cache them in separate keys.
        """
        user = await self.db['user'].find_one({"_id": ObjectId(user_id)})
        print("USER ", user)
        chat_ids = user['chats']  # Get all chat IDs for the user

        for chat_id in chat_ids:
            # Get all messages for this chat
            chat_messages = await self.db['message'].find({'chat_id': chat_id}).sort('timestamp', -1).to_list(
                length=200)

            # Cache messages in Redis under the key 'chat:{chat_id}:messages'
            await self.redis.set(f'chat:{chat_id}:messages', json.dumps(chat_messages))

        return "Messages for all chats loaded and cached successfully."

    async def get_messages_from_cache(self, chat_id):
        """
        Retrieve messages for a specific chat from cache.
        """
        cached_messages = await self.redis.get(f'chat:{chat_id}:messages')
        if cached_messages:
            return json.loads(cached_messages)
        return None

    async def add_message_to_cache(self, chat_id, message_data):
        try:
            """
            Add a new message to cache and database.
            """

            print("MESSAGE DATA: ", message_data)
            # Save the new message in the database

            default_values = {
                "is_read": False,
                "is_delivered": True,
            }

            for key, value in default_values.items():
                message_data.setdefault(key, value)

            message = await self.db['message'].insert_one(message_data)
            message_data['_id'] = str(message.inserted_id)

            # Retrieve cached messages for the specific chat
            cached_messages = await self.redis.get(f'chat:{chat_id}:messages')

            if cached_messages:
                cached_messages = json.loads(cached_messages)
            else:
                cached_messages = []

            # Add the new message to the cached messages
            cached_messages.append(message_data)

            # Save the updated messages in Redis cache
            await self.redis.set(f'chat:{chat_id}:messages', json.dumps(cached_messages))

            # Update the last message in the chat in the database
            await self.db['chat'].update_one(
                {"_id": ObjectId(chat_id)},
                {"$set": {"last_message": message.inserted_id}}
            )

            chat_participants = await self.redis.get(f'chat:{chat_id}:participants')
            print("CHAT PARTICIPANTS: ", chat_participants)
            return message_data, chat_participants
        except Exception as e:
            print(f"Error adding message to cache or to DB. Cache will be reloaded: {e}")
            return None

    async def delete_message_from_cache(self, chat_id, message_id):
        """
        Delete a message from cache.
        """
        cached_messages = await self.redis.get(f'chat:{chat_id}:messages')
        if cached_messages:
            cached_messages = json.loads(cached_messages)
            cached_messages = [msg for msg in cached_messages if msg['_id'] != message_id]
            # Save the updated messages in Redis cache
            await self.redis.set(f'chat:{chat_id}:messages', json.dumps(cached_messages))

    async def update_message_from_cache(self, chat_id, user_id, message_id=None, is_read = True, is_delivered = True):
        if message_id and is_read:
            await self.db['message'].update_one({"_id": ObjectId(message_id)}, {"$set": {"is_read": True}})
            print(f"NEW UPDATE: MESSAGE {message_id} WAS READ")
        if message_id and is_delivered:
            await self.db['message'].update_one({"_id": ObjectId(message_id)}, {"$set": {"is_delivered": True}})
            print(f"NEW UPDATE: MESSAGE {message_id} WAS DELIVERED")

        if not message_id:
            await self.db['message'].update_many({"chat_id": ObjectId(chat_id)}, {"$set": {"is_read": True, "is_delivered": True}})
            print(f"NEW UPDATE: ALL MESSAGES IN CHAT {chat_id} WAS READ AND DELIVERED")

        cached_messages = await self.redis.get(f'chat:{chat_id}:messages')
        if cached_messages:
            cached_messages = json.loads(cached_messages)
            for msg in cached_messages:
                if msg["sender"] != user_id:
                    if message_id and msg['_id'] == message_id:
                        msg['is_read'] = is_read
                        msg['is_delivered'] = is_delivered
                    else:
                        msg['is_read'] = True
                        msg['is_delivered'] = True
            # Save the updated messages in Redis cache
            await self.redis.set(f'chat:{chat_id}:messages', json.dumps(cached_messages))
            print("CASH UPDATED: MESSAGE ", message_id)

    async def clear_chat_cache(self, chat_id):
        """
        Clear cache for a specific chat.
        """
        await self.redis.delete(f'chat:{chat_id}:messages')

    async def clear_user_cache(self, user_id):
        """
        Clear all cache related to a specific user.
        """
        user = await self.db['user'].find_one({"_id": ObjectId(user_id)})
        for chat_id in user['chat']:
            await self.clear_chat_cache(chat_id)

        await self.redis.delete(f'user:{user_id}:messages')  # Clear user's general messages cache if any.
