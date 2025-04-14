import json
from bson import ObjectId

class ChatCacheManager:
    def __init__(self, db_manager, cache_manager):

        self.db = db_manager
        self.cache = cache_manager

    async def load_chat_info(self, chat_id: str):

        chat = await self.db.read_documents('chat', {'_id': ObjectId(chat_id)})
        print("Chat Loaded: ", chat)
        if not chat:
            return None

        participants = []
        if 'participants' in chat:
            for object_id in chat['participants']:
                user = await self.db.read_documents('user', {'_id': object_id})
                participants.append(user)
        chat['participants'] = participants

        if 'last_message' in chat:
            message = await self.db.read_documents('message', {'_id': ObjectId(chat['last_message'])})
            chat['last_message'] = message
        await self.cache.set_data(f'chat:{chat_id}:info', chat)
        return chat

    async def add_new_chat(self, chat_data: dict):

        result = await self.db.add_document('chat', chat_data)
        chat_data = result
        chat_data['_id'] = str(result.inserted_id)
        await self.cache.set_data(f'chat:{str(result.inserted_id)}:info', json.dumps(chat_data))
        return chat_data

    async def get_chat_info(self, chat_id: str):

        cached = await self.cache.data_get(f'chat:{chat_id}:info')
        return json.loads(cached) if cached else None

    async def update_chat_info(self, chat_id: str, update: dict):

        await self.db.update_document('chat', {'_id': ObjectId(chat_id)}, update)
        chat_info = await self.get_chat_info(chat_id) or {}
        chat_info.update(update)
        await self.cache.set_data(f'chat:{chat_id}:info',chat_info)
        return chat_info

    async def delete_chat_info(self, chat_id: str):

        await self.cache.delete(f'chat:{chat_id}:info')
