import os

import redis
import motor.motor_asyncio
import json
from bson import ObjectId
import os
os.getenv("DB_URL", "mongodb://admin:admin@localhost:27017")


class ChatCacheManager:
    def __init__(self, db_url=os.getenv("DB_URL", "mongodb://admin:admin@localhost:27017"), redis_url='redis://localhost:6379'):
        # Connect to MongoDB
        self.client = motor.motor_asyncio.AsyncIOMotorClient(db_url)
        self.db = self.client['whatsapp_clone']

        # Connect to Redis
        self.redis = redis.asyncio.StrictRedis.from_url(redis_url)

    async def load_user_chats_to_cache(self, user_id):
        """
        Load and cache information about all the chats the user is part of.
        """
        user = await self.db['user'].find_one({"_id": ObjectId(user_id)})
        chat_ids = user['chats']  # Get the list of chat IDs for the user

        chat_data = {}
        for chat_id in chat_ids:
            # Retrieve chat data from MongoDB
            chat = await self.db['chat'].find_one({"_id": ObjectId(chat_id)})
            chat_data[chat_id] = {
                'name': chat['name'],
                'participants': await self.get_participants_for_chat(chat_id),
                'last_message': await self.get_last_message_for_chat(chat_id),
                'is_group': chat['is_group'],
                'created_at': str(chat['created_at'])
            }

            # Cache the chat data in Redis
            await self.redis.set(f'chat:{chat_id}:info', json.dumps(chat_data[chat_id]))
        chat_data_for_chat_list = [
            {"chat_id": str(key), **value} for key, value in chat_data.items()
        ]
        print("\n !!!!!!!!!!!!!!!!!!!!!! CHAT_DATA_FOR_CHAT_LIST: ", chat_data_for_chat_list, "\n")
        await self.redis.set(f'user:{user_id}:chats', json.dumps(chat_data_for_chat_list))

        print("CHAT DATA: ", chat_data)
        return chat_data

    async def get_chat_from_cache(self, chat_id):
        """
        Retrieve chat data from Redis cache.
        """
        cached_chat = await self.redis.get(f'chat:{chat_id}:info')
        if cached_chat:
            return json.loads(cached_chat)
        return None

    async def get_chat_list_for_user_from_cache(self, user_id):
        """
        Retrieve chat list for a specific user from Redis cache.
        """
        cached_chat_list = await self.redis.get(f'user:{user_id}:chats')
        if cached_chat_list:
            return json.loads(cached_chat_list)
        return None


    async def update_chat_in_cache(self, chat_id, updated_data):
        """
        Update chat data in Redis cache.
        """
        cached_chat = await self.redis.get(f'chat:{chat_id}:info')
        if cached_chat:
            cached_chat = json.loads(cached_chat)
            cached_chat.update(updated_data)
            await self.redis.set(f'chat:{chat_id}:info', json.dumps(cached_chat))

            # Optionally, update the chat in MongoDB as well
            await self.db['chat'].update_one(
                {"_id": ObjectId(chat_id)},
                {"$set": updated_data}
            )

    async def delete_chat_from_cache(self, chat_id):
        """
        Delete chat data from Redis cache.
        """
        await self.redis.delete(f'chat:{chat_id}:info')

    async def get_participants_for_chat(self, chat_id):
        """
        Retrieve participants for a specific chat and cache them.
        """
        cached_participants = await self.redis.get(f'chat:{chat_id}:participants')
        # if cached_participants:
        #     return json.loads(cached_participants)

        chat = await self.db['chat'].find_one({"_id": ObjectId(chat_id)})
        participants = []
        for user_id in chat['participants']:
            user = await self.db['user'].find_one({"_id": ObjectId(user_id)})
            participant_data = {
                'id': user['_id'].__str__(),
                'name': user['name'],
                'profile_picture': user['profile_picture'],
                'email': user['email'],
                'phone_number': user['phone_number']
            }
            participants.append(participant_data)

        print(f"PARTICIPANTS DATA FOR CHAT {chat_id}: ", participants)
        # Cache the participants for this chat
        await self.redis.set(f'chat:{chat_id}:participants', json.dumps(participants))
        return participants

    async def get_last_message_for_chat(self, chat_id):
        """
        Retrieve the last message for a specific chat and cache it.
        """
        cached_last_message = await self.redis.get(f'chat:{chat_id}:last_message')
        # if cached_last_message:
        #     return json.loads(cached_last_message)

        chat = await self.db['chat'].find_one({"_id": ObjectId(chat_id)})
        last_message_id = chat.get('last_message')
        if last_message_id:
            message = await self.db['message'].find_one({"_id": ObjectId(last_message_id)})
            try:
                last_message_data = {
                    'sender': message['sender'].__str__(),
                    'text': message['text'],
                    'timestamp': str(message['timestamp'])
                }
            except Exception as e:
                last_message_data = {
                    "sender": None,
                    "text": None,
                    "timestamp": None
                }
            # Cache the last message for this chat
            await self.redis.set(f'chat:{chat_id}:last_message', json.dumps(last_message_data))
            return last_message_data
        return None

    async def clear_user_chats_cache(self, user_id):
        """
        Clear cached chat data for a specific user.
        """
        user = await self.db['user'].find_one({"_id": ObjectId(user_id)})
        for chat_id in user['chats']:
            await self.delete_chat_from_cache(chat_id)
            await self.redis.delete(f'chat:{chat_id}:participants')
            await self.redis.delete(f'chat:{chat_id}:last_message')
