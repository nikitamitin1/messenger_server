import json
from bson import ObjectId
from db_managers.mongo_db_manager import MongoDBManager
from .redis_cache_manager import RedisCacheManager

class MessageCacheManager:
    def __init__(self, db_manager: MongoDBManager, cache_manager: RedisCacheManager):
        self.db = db_manager
        self.cache = cache_manager
        self._unsync_messages_in_cache = 0
    async def load(self, chat_id: str):

        messages = await self.db.read_documents(
            collection='message',
            query={'chat_id': ObjectId(chat_id)},
            sort=[('timestamp', -1)],
            limit=200
        )
        await self.cache.set_data(f'chat:{chat_id}:messages', messages)
        return messages

    async def get_messages(self, chat_id: str):

        cached = await self.cache.get_data(f'chat:{chat_id}:messages')
        return cached if cached else []

    async def add_message(self, chat_id: str, message_data: dict):

        defaults = {"is_read": False, "is_delivered": True}
        for key, value in defaults.items():
            message_data.setdefault(key, value)
        try:
            result = await self.db.add_document('message', message_data)
            message_data['_id'] = str(result.inserted_id)
            messages = await self.cache.get_data(chat_id)
            messages = [] if messages is None else messages
            messages.append(message_data)
            await self.cache.set_data(f'chat:{chat_id}:messages', messages)

        except Exception as e:
            print(e)
        return message_data

    async def update_message(self, chat_id: str, message_id: str = None, update: dict = None):

        if update is None:
            update = {"is_read": True, "is_delivered": True}
        if message_id:
            await self.db.update_document('message', {"_id": ObjectId(message_id)}, update)
        else:
            await self.db.update_document('message', {"chat_id": ObjectId(chat_id)}, update)
        messages = await self.cache.get_data(chat_id)
        if messages:
            for msg in messages:
                if message_id and msg['_id'] == message_id:
                    msg.update(update)
                elif not message_id:
                    msg.update(update)
            await self.cache.set_data(f'chat:{chat_id}:messages', messages)
        return messages

    async def clear_chat_cache(self, chat_id: str):

        await self.cache.delete_data(f'chat:{chat_id}:messages')
