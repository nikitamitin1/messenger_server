from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse
from pydantic import BaseModel
from ws_manager import manager
from cache_managers.cache_message import MessageCacheManager
from cache_managers.cache_chat import ChatCacheManager
from cache_managers.cache_user import UserCacheManager


import json
from delivery.event_consumer import process_user_notifications
from delivery.event_producer import produce_ack_event, produce_message_event
app = FastAPI()
import asyncio

# WebSocket manage


#TODO make the acknowledgement logic for delivery/read for all the participants of chat

#TODO and test chat listener

#TODO figure out how the notification processing could be build to send a sufficient information to client for updating chat list
# maybe it could be done using the kafka queue notifications count

#TODO think how to manage user notification that were not elaborated

#TODO STORE ALL WS CONNECTIONS IN REDIS


users_db = {}
chats_db = ChatCacheManager()
messages_db = MessageCacheManager()

contacts_db = {}  # Store contacts (for demo purposes)


class Message(BaseModel):
    sender_id: str
    chat_id: str
    text: str


@app.websocket("/ws/{user_id}")
async def websocket_endpoint(websocket: WebSocket, user_id: str):
    """
    Handle the WebSocket connection and register events for user actions.
    """
    await manager.connect(websocket, user_id)

    await messages_db.load_messages_to_cache(user_id)
    await chats_db.load_user_chats_to_cache(user_id)

    asyncio.create_task(process_user_notifications(user_id))
    print("TASK CREATED")

    try:
        # Listen for incoming WebSocket messages
        while True:
            data = await websocket.receive_text()
            data = json.loads(data)
            print("\n Received data:", data, "\n")

            # Assume the data is in JSON format
            action = data.get("action")
            if action:
                if action == "send_message":
                    await handle_send_message(data["data"], user_id)
                elif action == "open_chat":
                    await handle_open_chat(user_id, data["data"]["chat_id"])
                elif action == "update_profile":
                    await handle_update_profile(data["data"])
                elif action == "delete_message":
                    await handle_delete_message(data["data"])
                elif action == "create_chat":
                    await handle_create_chat(data["data"])
                elif action == "delete_chat":
                    await handle_delete_chat(data["data"])
                elif action == "add_contact":
                    await handle_add_contact(data["data"])
                elif action == "remove_contact":
                    await handle_remove_contact(data["data"])
                elif action == "update_contact":
                    await handle_update_contact(data["data"])
                else:
                    await manager.send_personal_message("Unknown action.", user_id)
    except WebSocketDisconnect:
        manager.disconnect(websocket, user_id)
        print(f"User {user_id} disconnected.")


async def handle_send_message(data, user_id):
    """
    Handle the event of sending a message.
    """

    print("MESSAGE HAS BEEN RECIEVED BY WEBSOCKET: ", data)
    # Parse the data
    sender_id = data.get("sender")
    chat_id = data.get("chat")
    text = data.get("text")

    # Update message cache and message collection in db (here we update cache and db)
    message_data = {
        "sender": sender_id,
        "chat": chat_id,
        "text": text
    }

    chat_participants = await chats_db.get_participants_for_chat(chat_id)
    print("CHAT PARTICIPANTS LOADED: ", chat_participants)
    for user in chat_participants:
        if user["id"] != user_id:
            await produce_message_event(user_producer_id=user_id, user_consumer_id=user["id"], chat_id=chat_id, text=text)
async def handle_open_chat(user_id, chat_id):
    """
    Handle the event of opening a chat.
    """
    # Add the chat to the user's list of open chats
    chat_participants = await chats_db.get_participants_for_chat(chat_id)
    for user in chat_participants:
        if user["id"] != user_id:
            await produce_ack_event(user_producer_id=user_id, user_consumer_id=user["id"], chat_id=chat_id)

async def handle_mark_as_read(data):
    """
    Handle the event of marking a message as read.
    """
    message_id = data.get("message_id")
    # Mark the message as read (for simplicity, we update it in an in-memory store)
    if message_id in messages_db:
        messages_db[message_id]["is_read"] = True
        await manager.broadcast(f"Message {message_id} marked as read.")

async def handle_update_profile(data):
    """
    Handle updating the user's profile.
    """
    user_id = data.get("user_id")
    new_name = data.get("new_name")
    new_profile_picture = data.get("new_profile_picture")

    # Update the user profile in the in-memory database
    if user_id in users_db:
        users_db[user_id]["name"] = new_name
        users_db[user_id]["profile_picture"] = new_profile_picture
        await manager.broadcast(f"User {user_id}'s profile updated.")

async def handle_delete_message(data):
    """
    Handle deleting a message.
    """
    message_id = data.get("message_id")
    if message_id in messages_db:
        del messages_db[message_id]
        await manager.broadcast(f"Message {message_id} deleted.")

async def handle_create_chat(data):
    """
    Handle creating a new chat.
    """
    chat_name = data.get("chat_name")
    participants = data.get("participants")

    # Create a new chat and store it
    chat_id = str(len(chats_db) + 1)
    chats_db[chat_id] = {"name": chat_name, "participants": participants}

    # Send the chat creation event to the user
    await manager.broadcast(f"New chat created: {chat_name}")

async def handle_delete_chat(data):
    """
    Handle deleting a chat.
    """
    chat_id = data.get("chat_id")
    if chat_id in chats_db:
        del chats_db[chat_id]
        await manager.broadcast(f"Chat {chat_id} deleted.")

async def handle_add_contact(data):
    """
    Handle adding a new contact.
    """
    user_id = data.get("user_id")
    contact_id = data.get("contact_id")

    # Add the contact to the user's contact list (for simplicity, we store in memory)
    if user_id not in contacts_db:
        contacts_db[user_id] = []
    contacts_db[user_id].append(contact_id)
    await manager.broadcast(f"User {user_id} added contact {contact_id}.")

async def handle_remove_contact(data):
    """
    Handle removing a contact.
    """
    user_id = data.get("user_id")
    contact_id = data.get("contact_id")

    if user_id in contacts_db and contact_id in contacts_db[user_id]:
        contacts_db[user_id].remove(contact_id)
        await manager.broadcast(f"User {user_id} removed contact {contact_id}.")

async def handle_update_contact(data):
    """
    Handle updating a contact's information.
    """
    user_id = data.get("user_id")
    contact_id = data.get("contact_id")
    new_contact_info = data.get("new_contact_info")

    # Update the contact's info (for simplicity, we modify the in-memory store)
    if user_id in contacts_db and contact_id in contacts_db[user_id]:
        contacts_db[user_id][contact_id] = new_contact_info
        await manager.broadcast(f"User {user_id} updated contact {contact_id}.")


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)