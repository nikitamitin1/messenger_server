import json
from datetime import datetime
import json
import uuid
from datetime import datetime


class NotificationCacheManager:
    """
    NotificationCacheManager is responsible for performing CRUD operations on user notifications.
    Notifications are stored in a Redis list using the key pattern: user:{user_id}:notifications.
    This class relies on a cache manager (e.g., an instance of RedisCacheManager) that provides list operations.
    """

    def __init__(self, cache_manager):
        self.cache = cache_manager

    async def create_notification(self, user_id: str, notification_data: dict):
        """
        Creates a new notification for the specified user. If certain fields are missing in the provided
        notification_data, default values will be assigned.

        Parameters:
            user_id (str): The unique identifier of the user.
            notification_data (dict): A dictionary containing notification information.
                Expected fields include:
                    - event_type: The type of event (e.g., "message").
                    - chat_id: The chat identifier (if applicable).
                    - message_id: The message identifier.
                    - sender_id: The identifier of the sender.
                    - message_text: The content of the message.
                    - timestamp: The time the notification was created (ISO format). If not provided, the current UTC time is used.
                    - is_read: A boolean indicating whether the notification has been read (default is False).
                    - Additional custom fields can be included as needed.

        The method generates a unique notification id (if not already present) and pushes the notification into the Redis list.
        """
        if "id" not in notification_data:
            notification_data["id"] = str(uuid.uuid4())
        if "timestamp" not in notification_data:
            notification_data["timestamp"] = datetime.utcnow().isoformat()
        if "is_read" not in notification_data:
            notification_data["is_read"] = False

        key = f"user:{user_id}:notifications"
        await self.cache.rpush_data(key, notification_data)

    async def update_notification(self, user_id: str, notification_id: str, updated_data: dict):
        """
        Updates an existing notification identified by its unique id.

        Parameters:
            user_id (str): The unique identifier of the user.
            notification_id (str): The unique identifier of the notification to update.
            updated_data (dict): A dictionary containing fields to update in the notification.

        The method retrieves the list of notifications from Redis, searches for the notification with the given id,
        updates its fields with the provided data, and then uses the lset operation to update the notification in the Redis list.
        If no matching notification is found, a ValueError is raised.
        """
        key = f"user:{user_id}:notifications"
        notifications = await self.cache.lrange_data(key, 0, -1)
        updated = False
        for i, notif in enumerate(notifications):
            if notif.get("id") == notification_id:
                notif.update(updated_data)
                await self.cache.lset_data(key, i, notif)
                updated = True
                break
        if not updated:
            raise ValueError(f"Notification with id {notification_id} not found for user {user_id}")

    async def delete_notification_by_id(self, user_id: str, notification_id: str):
        """
        Deletes a notification by its unique identifier.

        Parameters:
            user_id (str): The unique identifier of the user.
            notification_id (str): The unique identifier of the notification to delete.

        The method retrieves the entire list of notifications, filters out the notification with the specified id,
        deletes the existing list in Redis, and then re-pushes the filtered notifications back into Redis.
        """
        key = f"user:{user_id}:notifications"
        notifications = await self.get_notifications(user_id)
        new_notifications = [notif for notif in notifications if notif.get("id") != notification_id]
        await self.cache.delete_data(key)
        for notif in new_notifications:
            await self.cache.rpush_data(key, notif)

    async def delete_notifications_by_chat_id(self, user_id: str, chat_id: str):
        """
        Deletes all notifications associated with a specific chat.

        Parameters:
            user_id (str): The unique identifier of the user.
            chat_id (str): The identifier of the chat whose notifications should be removed.

        The method filters out notifications that contain the specified chat_id and then re-pushes the remaining notifications.
        """
        key = f"user:{user_id}:notifications"
        notifications = await self.get_notifications(user_id)
        new_notifications = [notif for notif in notifications if notif.get("chat_id") != chat_id]
        await self.cache.delete_data(key)
        for notif in new_notifications:
            await self.cache.rpush_data(key, notif)

    async def get_notifications(self, user_id: str) -> list:
        """
        Retrieves all notifications for the specified user.

        Parameters:
            user_id (str): The unique identifier of the user.

        Returns:
            list: A list of notification dictionaries retrieved from the Redis list.
        """
        key = f"user:{user_id}:notifications"
        notifications = await self.cache.lrange_data(key, 0, -1)
        return notifications


class NotificationAnalyticsManager:
    """
    NotificationAnalyticsManager aggregates notification data and computes an analytical summary for a user.
    The summary is structured with the following keys:

      - system: Metadata about the summary (e.g., generation time and version).
      - system_notifications: A list of system-wide notifications (those that do not have a chat_id).
      - chat_states: A list of chat-specific states containing, for each chat the user is part of:
          - chat_id: The unique identifier of the chat.
          - last_message: The last message received in that chat (retrieved from the chat object).
          - unread_count: The number of unread notifications for that chat (0 if none exist).
      - total_unread: The total number of unread notifications across all chats and system notifications.

    The computed summary is stored in Redis using a provided cache manager under the key: user:{user_id}:summary.
    """

    def __init__(self, notification_cache_manager, chat_cache_manager, cache_manager, user_manager):
        """
        Initializes the NotificationAnalyticsManager with the required managers.

        Parameters:
            notification_cache_manager: An instance of NotificationCacheManager used to retrieve user notifications.
            chat_cache_manager: An instance of ChatCacheManager used to retrieve chat information.
            cache_manager: An instance of RedisCacheManager (or similar) used to store the summary.
        """
        self.notification_cache_manager = notification_cache_manager
        self.chat_cache_manager = chat_cache_manager
        self.cache = cache_manager
        self.user_manager = user_manager

    async def compute_summary(self, user_id: str) -> dict:
        """
        Computes the analytical summary for the specified user, including all chats the user is part of.
        Even if a chat does not have any unread notifications, its last message is included with unread_count = 0.

        The summary structure is as follows:

        {
            "system": {
                "generated_at": "<ISO timestamp>Z",
                "version": "1.0"
            },
            "system_notifications": [
                { ... },  // system-wide notifications (without chat_id)
                ...
            ],
            "chat_states": [
                {
                    "chat_id": "<chat identifier>",
                    "last_message": { ... },   // Last message details from the chat object
                    "unread_count": <number>   // 0 if there are no unread notifications
                },
                ...
            ],
            "total_unread": <total unread count across chats and system notifications>
        }

        How it works:
          1. Retrieve all notifications for the user via NotificationCacheManager.
          2. Iterate over notifications to separate chat-specific notifications from system notifications.
          3. For chat-specific notifications, aggregate unread counts by chat_id.
          4. Retrieve the list of all chats for the user via ChatCacheManager. This ensures that all chats are represented,
             even if there are no notifications for some chats (they get an unread_count of 0).
          5. For each chat in the list, include the last_message (from the chat object) and use the aggregated unread count (or 0 if none).
          6. Compute total_unread from both system notifications and chat notifications.
          7. Build the final summary and store it in Redis using the provided cache manager.

        Parameters:
            user_id (str): The unique identifier of the user.

        Returns:
            dict: The computed summary.
        """
        # Retrieve all notifications for the user.
        notifications = await self.notification_cache_manager.get_notifications(user_id)

        # Aggregate unread counts for chat-specific notifications and collect system notifications.
        chat_unread = {}  # mapping: chat_id -> unread count
        system_notifications = []
        total_unread = 0

        for notif in notifications:
            if notif["type"] == "message" and "chat_id" in notif and notif["chat_id"]:
                chat_id = notif["chat_id"]
                if not notif.get("is_read", False):
                    chat_unread[chat_id] = chat_unread.get(chat_id, 0) + 1
                    total_unread += 1
            else:
                system_notifications.append(notif)
                if not notif.get("is_read", False):
                    total_unread += 1

        # Retrieve the list of all chats for this user. It is assumed that get_chat_list_for_user_from_cache returns a list of chat objects.
        user_profile = await self.user_manager.get_user_profile(user_id)
        chat_list = user_profile.get("chats", [])
        chat_states = []
        # For each chat, include its last_message and the unread count (0 if no unread notifications exist).
        for chat in chat_list:
            # Use chat['chat_id'] if available, else use chat['_id']
            chat_id = chat.get("chat_id") or str(chat.get("_id"))
            unread_count = chat_unread.get(chat_id, 0)
            last_message = chat.get("last_message")  # Assuming the chat object includes a "last_message" field
            chat_states.append({
                "chat_id": chat_id,
                "last_message": last_message,
                "unread_count": unread_count
            })

        summary = {
            "system": {
                "generated_at": datetime.utcnow().isoformat() + "Z",
                "version": "1.0"
            },
            "system_notifications": system_notifications,
            "chat_states": chat_states,
            "total_unread": total_unread
        }

        # Store the summary in Redis under the key: user:{user_id}:summary
        await self.cache.set_data(f"user:{user_id}:summary", summary)
        return summary

    async def get_summary(self, user_id: str) -> dict:
        """
        Retrieves the stored summary for the user from Redis. If no summary is found, it computes one.

        Parameters:
            user_id (str): The unique identifier of the user.

        Returns:
            dict: The analytical summary.
        """
        summary = await self.cache.get_data(f"user:{user_id}:summary")
        if summary:
            return summary
        else:
            return await self.compute_summary(user_id)
