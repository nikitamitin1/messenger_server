import redis
import motor.motor_asyncio
import json
from bson import ObjectId
import os

class UserCacheManager:
    def __init__(self, db_url=os.getenv("DB_URL", "mongodb://admin:admin@localhost:27017"), redis_url='redis://localhost:6379'):
        # Connect to MongoDB
        self.client = motor.motor_asyncio.AsyncIOMotorClient(db_url)
        self.db = self.client['whatsapp_clone']

        # Connect to Redis
        self.redis = redis.asyncio.StrictRedis.from_url(redis_url)

    async def load_user_profile_to_cache(self, user_id):
        """
        Load user's profile data (name, email, phone number, etc.) and cache it in Redis.
        """
        user = await self.db['user'].find_one({"_id": ObjectId(user_id)})

        print(f"USER CHATS {user_id} CACHED: {user['chats']}")
        # Cache user profile data in Redis under 'user:{user_id}:profile'
        user_profile_data = {
            'name': user['name'],
            'email': user['email'],
            'phone_number': user['phone_number'],
            'profile_picture': user['profile_picture'],
            'chats': user['chats']
        }

        await self.redis.set(f'user:{user_id}:profile', json.dumps(user_profile_data))
        return user_profile_data

    async def get_user_profile_from_cache(self, user_id):
        """
        Retrieve user profile data from Redis cache.
        """
        cached_profile = await self.redis.get(f'user:{user_id}:profile')
        if cached_profile:
            return json.loads(cached_profile)
        return None

    async def update_user_profile_in_cache(self, user_id, updated_data):
        """
        Update user profile data in Redis cache.
        """
        cached_profile = await self.redis.get(f'user:{user_id}:profile')
        if cached_profile:
            cached_profile = json.loads(cached_profile)
            cached_profile.update(updated_data)

            # Save the updated profile data back to Redis
            await self.redis.set(f'user:{user_id}:profile', json.dumps(cached_profile))

            # Optionally, you could also update the data in MongoDB if necessary
            await self.db['user'].update_one(
                {"_id": ObjectId(user_id)},
                {"$set": updated_data}
            )

    async def delete_user_profile_from_cache(self, user_id):
        """
        Delete user profile data from Redis cache.
        """
        await self.redis.delete(f'user:{user_id}:profile')

    async def load_chat_participants_to_cache(self, user_id):
        """
        Load and cache the participants of all chats the user is part of.
        """
        user = await self.db['user'].find_one({"_id": ObjectId(user_id)})
        chat_ids = user['chats']

        participants_data = {}
        for chat_id in chat_ids:
            chat = await self.db['chat'].find_one({"_id": ObjectId(chat_id)})
            participants_data[chat_id] = []

            for user_id in chat['participants']:
                participant = await self.db['user'].find_one({"_id": ObjectId(user_id)})
                print("PARTICIPANT ID: ", participant['_id'].__str__())
                participant_data = {
                    'id': participant['_id'].__str__(),
                    'name': participant['name'],
                    'profile_picture': participant['profile_picture']
                }

                if not chat['is_group']:  # If private chat, add full information
                    participant_data.update({
                        'email': participant['email'],
                        'phone_number': participant['phone_number']
                    })

                participants_data[chat_id].append(participant_data)

            # Cache the participants in Redis for the specific chat
            await self.redis.set(f'chat:{chat_id}:participants', json.dumps(participants_data[chat_id]))

        return participants_data

    async def get_chat_participants_from_cache(self, chat_id):
        """
        Retrieve cached participants for a specific chat.
        """
        cached_participants = await self.redis.get(f'chat:{chat_id}:participants')
        if cached_participants:
            return json.loads(cached_participants)
        return None

    async def clear_user_cache(self, user_id):
        """
        Clear all cached data related to the user (profile, chat participants, etc.).
        """
        # Clear user profile cache
        await self.redis.delete(f'user:{user_id}:profile')

        # Clear chat participants cache for each chat the user belongs to
        user = await self.db['user'].find_one({"_id": ObjectId(user_id)})
        for chat_id in user['chats']:
            await self.redis.delete(f'chat:{chat_id}:participants')

        # Optionally, clear any other cached data related to the user (e.g., messages)
        await self.redis.delete(f'user:{user_id}:messages')
