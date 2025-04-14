import asyncio
import pytest
import json
from datetime import datetime, timezone
from bson import ObjectId

# Import modules – adjust the paths as needed.
from db_managers.mongo_db_manager import MongoDBManager
from cache_managers_upd.redis_cache_manager import RedisCacheManager
from cache_managers_upd.message_manager import MessageCacheManager
from cache_managers_upd.chat_manager import ChatCacheManager
from cache_managers_upd.user_manager import UserCacheManager

# Set up connection parameters – adjust if necessary.
MONGO_HOST = "admin:admin@localhost"
MONGO_PORT = 27017
MONGO_DB = "whatsapp_clone"
REDIS_URL = "redis://localhost:6379"

pytestmark = pytest.mark.asyncio

# ---------------------------------------------------------------------
# Fixtures: Create shared instances for MongoDB, Redis, and cache managers.
# ---------------------------------------------------------------------
@pytest.fixture(scope="module")
async def mongo_manager():
    mgr = MongoDBManager(MONGO_HOST, MONGO_PORT, MONGO_DB)
    await mgr.connect()
    print("MongoDBManager connected")
    yield mgr
    await mgr.close()

@pytest.fixture(scope="module")
def redis_manager():
    print("Redis URL:", REDIS_URL, "connected")
    return RedisCacheManager(REDIS_URL)

@pytest.fixture(scope="module")
def message_manager(mongo_manager, redis_manager):
    print("Message Manager URL connected")
    return MessageCacheManager(mongo_manager, redis_manager)

@pytest.fixture(scope="module")
def chat_manager(mongo_manager, redis_manager):
    print("Chat Manager connected")
    return ChatCacheManager(mongo_manager, redis_manager)

@pytest.fixture(scope="module")
def user_manager(mongo_manager, redis_manager):
    print("User Manager connected")
    return UserCacheManager(mongo_manager, redis_manager)

# ---------------------------------------------------------------------
# Test MongoDBManager: Adding and Reading a Document.
# ---------------------------------------------------------------------
async def test_mongo_db_add_and_read_document(mongo_manager: MongoDBManager):
    # Create a test document in a temporary collection.
    test_doc = {"name": "John Doe", "email": "john@example.com", "created_at": datetime.utcnow().isoformat()}
    result = await mongo_manager.add_document("test_collection", test_doc)
    assert result is not None
    inserted_id = result.inserted_id

    # Read back the document.
    docs = await mongo_manager.read_documents("test_collection", {"_id": inserted_id})
    assert docs is not None
    assert len(docs) >= 1
    print("MongoDB Read Document:", docs)

# ---------------------------------------------------------------------
# Test RedisCacheManager: set, get, delete, and list functions.
# ---------------------------------------------------------------------
async def test_redis_set_get_delete(redis_manager: RedisCacheManager):
    test_key = "test:key"
    test_data = {"foo": "bar", "num": 42}
    await redis_manager.set_data(test_key, test_data)
    data = await redis_manager.get_data(test_key)
    assert data is not None
    assert data == test_data

    # Test list operations.
    list_key = "test:list"
    await redis_manager.delete_data(list_key)
    await redis_manager.rpush_data(list_key, {"item": 1})
    await redis_manager.rpush_data(list_key, {"item": 2})
    items = await redis_manager.lrange_data(list_key, 0, -1)
    assert isinstance(items, list)
    assert len(items) == 2
    await redis_manager.lset_data(list_key, 0, {"item": 10})
    items_after = await redis_manager.lrange_data(list_key, 0, -1)
    assert items_after[0]["item"] == 10
    await redis_manager.delete_data(test_key)
    await redis_manager.delete_data(list_key)

# ---------------------------------------------------------------------
# Test MessageCacheManager: add, get, update, and clear messages.
# ---------------------------------------------------------------------
async def test_message_cache_manager_add_get_clear(message_manager: MessageCacheManager):
    # Create a fake chat_id. In a real test, you might create a chat document first.
    chat_id = "1111111"
    await message_manager.clear_chat_cache(chat_id)  # Ensure cache is empty.

    message_data = {
        "sender": "test_user",
        "chat": chat_id,
        "text": "Hello, integration test!"
    }
    new_message = await message_manager.add_message(chat_id, message_data)
    assert new_message.get("sender") == "test_user"
    messages = await message_manager.get_messages(chat_id)
    print("MESSAGES LOADED: ", messages)
    assert isinstance(messages, list)
    assert any(msg.get("text") == "Hello, integration test!" for msg in messages)
    updated_messages = await message_manager.update_message(chat_id, new_message.get("_id"), {"is_read": True})
    for msg in updated_messages:
        if msg.get("_id") == new_message.get("_id"):
            assert msg.get("is_read") is True
    await message_manager.clear_chat_cache(chat_id)
    messages_after_clear = await message_manager.get_messages(chat_id)
    assert messages_after_clear == []

# ---------------------------------------------------------------------
# Test UserCacheManager: load, get, update, and delete user profile.
# ---------------------------------------------------------------------
async def test_user_cache_manager_load_update_get_delete(user_manager: UserCacheManager, mongo_manager: MongoDBManager):
    test_user = {
        "name": "Test User",
        "email": f"testuser_{datetime.utcnow().timestamp()}@example.com",
        "phone_number": "5551234567",
        "profile_picture": "http://example.com/avatar.png",
        "chats": []
    }
    result = await mongo_manager.add_document("user", test_user)
    user_id = str(result.inserted_id)
    profile = await user_manager.load_user_profile(user_id)
    assert profile is not None
    assert profile.get("name") == "Test User"
    cached_profile = await user_manager.get_user_profile(user_id)
    assert cached_profile is not None
    assert cached_profile.get("email") == test_user["email"]
    update_data = {"name": "Updated Test User"}
    updated_profile = await user_manager.update_user_profile(user_id, update_data)
    assert updated_profile.get("name") == "Updated Test User"
    await user_manager.delete_user_profile(user_id)
    deleted_profile = await user_manager.get_user_profile(user_id)
    assert deleted_profile is None

# ---------------------------------------------------------------------
# Test ChatCacheManager: load, get, update, and delete chat info.
# ---------------------------------------------------------------------
async def test_chat_cache_manager_load_get_update_delete(chat_manager: ChatCacheManager, mongo_manager: MongoDBManager):
    test_chat = {
        "name": "Integration Test Chat",
        "participants": [ObjectId()],
        "is_group": False,
        "last_message": None,
        "created_at": datetime.utcnow().isoformat()
    }
    result = await mongo_manager.add_document("chat", test_chat)
    chat_id = str(result.inserted_id)
    chat_info = await chat_manager.load_chat_info(chat_id)
    assert chat_info is not None
    assert chat_info.get("name") == "Integration Test Chat"
    cached_info = await chat_manager.get_chat_info(chat_id)
    assert cached_info is not None
    update_data = {"name": "Updated Chat Name"}
    updated_info = await chat_manager.update_chat_info(chat_id, update_data)
    assert updated_info.get("name") == "Updated Chat Name"
    await chat_manager.delete_chat_info(chat_id)
    deleted_info = await chat_manager.get_chat_info(chat_id)
    assert deleted_info is None

# ---------------------------------------------------------------------
# Run tests if executed as main.
# ---------------------------------------------------------------------
if __name__ == "__main__":
    async def main():
        # Create instances manually (simulate fixtures).
        mongo_mgr = MongoDBManager(MONGO_HOST, MONGO_PORT, MONGO_DB)
        await mongo_mgr.connect()
        redis_mgr = RedisCacheManager(REDIS_URL)
        msg_mgr = MessageCacheManager(mongo_mgr, redis_mgr)
        cht_mgr = ChatCacheManager(mongo_mgr, redis_mgr)
        usr_mgr = UserCacheManager(mongo_mgr, redis_mgr)

        print("Running MongoDBManager test...")
        await test_mongo_db_add_and_read_document(mongo_mgr)
        print("Running RedisCacheManager test...")
        await test_redis_set_get_delete(redis_mgr)
        print("Running MessageCacheManager test...")
        await test_message_cache_manager_add_get_clear(msg_mgr)
        print("Running UserCacheManager test...")
        await test_user_cache_manager_load_update_get_delete(usr_mgr, mongo_mgr)
        print("Running ChatCacheManager test...")
        await test_chat_cache_manager_load_get_update_delete(cht_mgr, mongo_mgr)

        await mongo_mgr.close()

    asyncio.run(main())
