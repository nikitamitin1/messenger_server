import json
from bson import ObjectId

class UserCacheManager:
    def __init__(self, db_manager, cache_manager):
        """
        Initializes the UserCacheManager with both the database manager and the cache manager.

        Parameters:
            db_manager: An object used to interact with MongoDB, which should implement methods such as
                        read_documents and update_document.
            cache_manager: An object used to interact with Redis, which should implement methods such as
                           set_data, get_data, and delete_data.
        """
        self.db = db_manager
        self.cache = cache_manager

    async def load_user_profile(self, user_id: str):
        """
        Loads the user's profile from the database and caches it in Redis.

        This method fetches the user's profile from the 'user' collection in MongoDB by the given user_id,
        constructs a profile dictionary with key information such as name, email, phone number, profile picture,
        and associated chats, then caches this profile under the key "user:{user_id}:profile" in Redis.

        Parameters:
            user_id (str): The unique identifier for the user.

        Returns:
            dict or None: A dictionary representing the user's profile if found, else None.
        """
        # Retrieve user documents from the 'user' collection by matching the provided user_id.
        user = await self.db.read_documents('user', {'_id': ObjectId(user_id)})
        if not user:
            return None

        # Construct a user profile dictionary from the retrieved data.
        profile = {
            'name': user.get('name'),
            'email': user.get('email'),
            'phone_number': user.get('phone_number'),
            'profile_picture': user.get('profile_picture'),
            'chats': user.get('chats', [])
        }

        # Cache the user profile in Redis under the key "user:{user_id}:profile".
        await self.cache.set_data(f'user:{user_id}:profile', profile)
        return profile

    async def get_user_profile(self, user_id: str):
        """
        Retrieves the user's profile from the Redis cache.

        This method attempts to retrieve the user's profile that was previously cached under the key
        "user:{user_id}:profile". It returns the profile as a dictionary, or None if no profile exists in the cache.

        Parameters:
            user_id (str): The unique identifier for the user.

        Returns:
            dict or None: The user's profile as a dictionary if available; otherwise, None.
        """
        cached = await self.cache.get_data(f'user:{user_id}:profile')
        return json.loads(cached) if cached else None

    async def update_user_profile(self, user_id: str, updated_data: dict):
        """
        Updates the user's profile information in the database and updates the cached profile in Redis.

        First, this method updates the corresponding document in the MongoDB 'user' collection with the provided
        updated fields. It then retrieves the current profile from the cache (or initializes it as an empty dictionary),
        updates it with the new data, and finally writes the updated profile back to Redis.

        Parameters:
            user_id (str): The unique identifier for the user.
            updated_data (dict): A dictionary containing the fields to update in the user's profile.

        Returns:
            dict: The updated user profile.
        """
        # Update the user's document in the MongoDB 'user' collection.
        await self.db.update_document('user', {'_id': ObjectId(user_id)}, updated_data)

        # Retrieve the current cached profile; if missing, initialize as an empty dictionary.
        profile = await self.get_user_profile(user_id) or {}
        profile.update(updated_data)

        # Update the cached profile in Redis.
        await self.cache.set_data(f'user:{user_id}:profile', profile)
        return profile

    async def delete_user_profile(self, user_id: str):
        """
        Deletes the user's profile from the Redis cache.

        This method removes the profile associated with the specified user_id from the Redis cache, effectively
        clearing the user's profile data from the cache.

        Parameters:
            user_id (str): The unique identifier for the user.
        """
        await self.cache.delete_data(f'user:{user_id}:profile')
