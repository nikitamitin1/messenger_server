import json
import uuid
from datetime import datetime
from bson import ObjectId
import redis.asyncio as redis
from .cache_chat import ChatCacheManager

chat_cache_manager = ChatCacheManager()

# Create an asynchronous Redis connection with decode_responses=True
# r = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)

class NotificationCacheManager:
    """
    This class manages user notifications and maintains a summary of chat notifications.

    Notifications are stored in a Redis list under the key:
        user:{user_id}:notifications

    The summary (chat list with last message info and global unread count) is stored as a JSON
    string under the key:
        user:{user_id}:summary
    """
    r = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)

    def __init__(self):
        pass

    # ---------- Notification CRUD Methods ----------


    @staticmethod
    async def default_summary_for_user_chats(user_id: str):
        """
        Create the summary for a user on session start.
        """

        chat_list = await chat_cache_manager.get_chat_list_for_user_from_cache(user_id)
        print("\033[94mCHAT LIST FROM CACHE\033[94m")
        default_summary = {
            "chats": [],
            "global_unread": 0}
        for chat in chat_list:
            print("!!!!!!!! CHAT ITEM : ", chat)
            summary_for_chat = {
                "chat_id": chat["chat_id"],
                "last_message": chat["last_message"],
                "unread_count": 0
            }
            default_summary["chats"].append(summary_for_chat)
        await NotificationCacheManager.r.set(f"user:{user_id}:summary", json.dumps(default_summary))
        # final_summary = await self.update_summary(user_id)
        return default_summary

    @staticmethod
    async def add_notification(user_id: str, notification_data: dict):
        """
        Add a new notification for a user and update the summary.
        Generates a unique notification id if not provided.
        Expected notification_data fields:
            - event_type (e.g., "message")
            - chat_id
            - message_id
            - sender_id
            - message_text
            - timestamp (ISO format string; if not provided, current UTC time is used)
            - is_read (boolean, default False)
        """
        if "id" not in notification_data:
            notification_data["id"] = str(uuid.uuid4())

        if "timestamp" not in notification_data:
            notification_data["timestamp"] = datetime.utcnow().isoformat()

        if "is_read" not in notification_data:
            notification_data["is_read"] = False

        key = f"user:{user_id}:notifications"
        await NotificationCacheManager.r.rpush(key, json.dumps(notification_data))
        # await self.update_summary(user_id)

    @staticmethod
    async def get_notifications(user_id: str) -> list:
        """
        Retrieve all notifications for the given user.
        Returns a list of notifications (each as a dict).
        """
        key = f"user:{user_id}:notifications"
        notifications = await NotificationCacheManager.r.lrange(key, 0, -1)
        return [json.loads(n) for n in notifications]

    async def update_notification(self, user_id: str, notification_id: str, updated_data: dict):
        """
        Update a specific notification for a user.
        This method replaces fields of the notification with the provided updated_data.
        """
        key = f"user:{user_id}:notifications"
        notifications = await self.get_notifications(user_id)
        updated = False

        for i, notification in enumerate(notifications):
            if notification.get("id") == notification_id:
                notification.update(updated_data)
                await NotificationCacheManager.r.lset(key, i, json.dumps(notification))
                updated = True
                break

        if updated:
            pass
        else:
            raise ValueError(f"Notification with id {notification_id} not found for user {user_id}")

    async def delete_notification(self, user_id: str, notification_id: str):
        """
        Delete a specific notification for a user and update the summary.
        """
        key = f"user:{user_id}:notifications"
        notifications = await self.get_notifications(user_id)
        new_notifications = [n for n in notifications if n.get("id") != notification_id]
        await NotificationCacheManager.r.delete(key)
        for n in new_notifications:
            await NotificationCacheManager.r.rpush(key, json.dumps(n))
        # await self.update_summary(user_id)

    async def delete_notifications_by_chat_id(self, user_id: str, chat_id: str):
        """
        Delete all notifications for a user that belong to a specific chat.
        After deletion, update the summary.
        """
        key = f"user:{user_id}:notifications"
        notifications = await self.get_notifications(user_id)
        # Filter out notifications that belong to the specified chat_id.
        new_notifications = [n for n in notifications if n.get("chat_id") != chat_id]
        await NotificationCacheManager.r.delete(key)
        for n in new_notifications:
            await NotificationCacheManager.r.rpush(key, json.dumps(n))
        # await self.update_summary(user_id)

    # ---------- Summary Methods ----------

    import json

    async def update_summary(self, user_id: str):
        """
        Recalculate and update the summary for the user, updating only changed chats.
        """
        notifications = await self.get_notifications(user_id)
        print(" \n !!!! LIST OF NOTIF !!!!", notifications, " \n !!!! LIST OF NOTIF !!!!")
        summary_key = f"user:{user_id}:summary"

        # Get the existing summary, or initialize a new one if it doesn't exist
        existing_summary_str = await NotificationCacheManager.r.get(summary_key)
        existing_summary = json.loads(existing_summary_str) if existing_summary_str else {"chats": [],
                                                                                          "global_unread": 0}
        existing_chats = {chat["chat_id"]: chat for chat in existing_summary["chats"]}

        global_unread = 0  # Start with the existing global unread

        updated_chats = {}  # Track chats that have been updated
        if not notifications:
            final_summary = await self.default_summary_for_user_chats(user_id)
            return final_summary

        for notif in notifications:
            chat_id = notif.get("chat_id")
            if chat_id not in updated_chats:
                updated_chats[chat_id] = True

                # if chat_id not in existing_chats:
                #     # New chat, initialize it
                #     last_msg = await chat_cache_manager.get_last_message_for_chat(chat_id)
                #     existing_chats[chat_id] = {
                #         "chat_id": chat_id,
                #         "last_message": last_msg,
                #         "unread_count": 0
                #     }

                last_msg = await chat_cache_manager.get_last_message_for_chat(chat_id)
                existing_chats[chat_id]["last_message"] = last_msg
                existing_chats[chat_id]["unread_count"] = 0

            if chat_id in existing_chats and notif.get("event_type") == "message":
                print(f" !!!! NOTIF FOR CHAT {chat_id} DETECTED", notif)
                existing_chats[chat_id]["unread_count"] += 1
                global_unread += 1

        # Update the summary with the modified chat data
        final_summary = {
            "chats": list(existing_chats.values()),
            "global_unread": global_unread
        }

        await NotificationCacheManager.r.set(summary_key, json.dumps(final_summary))
        return final_summary

    async def get_summary(self, user_id: str) -> dict:
        """
        Retrieve the current summary for the user from Redis.
        """
        summary = await NotificationCacheManager.r.get(f"user:{user_id}:summary")
        if summary:
            return json.loads(summary)
        else:
            return await self.update_summary(user_id)

    # ---------- Clear All Notifications ----------
    @staticmethod
    async def clear_all_notifications(user_id: str):
        """
        Clear all notifications and summary for a user.
        """
        await NotificationCacheManager.r.delete(f"user:{user_id}:notifications")
        await NotificationCacheManager.r.delete(f"user:{user_id}:summary")
        keys = await NotificationCacheManager.r.keys(f"user:{user_id}:unread_count*")
        for key in keys:
            await NotificationCacheManager.r.delete(key)
