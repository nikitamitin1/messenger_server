import mongoengine as me
import datetime


# User Model - Represents a user in the system
class User(me.Document):
    name = me.StringField(required=True)
    email = me.StringField(unique=True, required=True)
    phone_number = me.StringField(unique=True, required=True)
    profile_picture = me.StringField(default="")  # URL or path to profile picture
    last_seen = me.StringField(default=str(datetime.datetime.utcnow))
    contacts = me.ListField(me.ReferenceField('self'), default=[])  # Self-reference to store contacts
    chats = me.ListField(me.ObjectIdField(), default=[])  # List of chat IDs the user participates in

    def add_contact(self, user):
        if user not in self.contacts:
            self.contacts.append(user)
            self.save()

    def remove_contact(self, user):
        if user in self.contacts:
            self.contacts.remove(user)
            self.save()

    def add_chat(self, chat):
        if chat.id not in self.chats:
            self.chats.append(chat.id)
            self.save()

    def remove_chat(self, chat):
        if chat.id in self.chats:
            self.chats.remove(chat.id)
            self.save()


# Chat Model - Represents a chat (group or individual)
class Chat(me.Document):
    name = me.StringField(required=True)  # Name of the group, or None for individual chat
    participants = me.ListField(me.ObjectIdField())  # List of participant IDs
    is_group = me.BooleanField(default=False)  # Whether this chat is a group chat
    last_message = me.ReferenceField('Message', default=None)
    created_at = me.StringField(default=str(datetime.datetime.utcnow))

    def add_participant(self, user):
        if user.id not in self.participants:
            self.participants.append(user.id)
            self.save()
            # Add chat to user
            user.add_chat(self)

    def remove_participant(self, user):
        if user.id in self.participants:
            self.participants.remove(user.id)
            self.save()
            # Remove chat from user
            user.remove_chat(self)


# Message Model - Represents a message sent in a chat
class Message(me.Document):
    sender = me.ReferenceField(User, required=True)
    chat = me.ReferenceField(Chat, required=True)
    text = me.StringField(default="")
    media = me.ReferenceField('Media')
    timestamp = me.StringField(default=str(datetime.datetime.utcnow))
    is_read = me.BooleanField(default=False)
    is_delivered = me.BooleanField(default=False)

    def mark_as_read(self):
        self.is_read = True
        self.save()

    def mark_as_delivered(self):
        self.is_delivered = True
        self.save()


# Media Model - Represents media files such as images, audio, or videos
class Media(me.Document):
    file_type = me.StringField(choices=['image', 'audio', 'video'], required=True)
    file_url = me.StringField(required=True)  # URL to the media file
    sender = me.ReferenceField(User, required=True)
    timestamp = me.StringField(default=str(datetime.datetime.utcnow))


# Notification Model - Represents notifications for events like new messages
class Notification(me.Document):
    recipient = me.ReferenceField(User, required=True)
    message = me.StringField(default="")
    timestamp = me.StringField(default=str(datetime.datetime.utcnow))
    is_read = me.BooleanField(default=False)
    type = me.StringField(choices=['message', 'alert', 'system'])

    def mark_as_read(self):
        self.is_read = True
        self.save()


# Contact Model - Represents a user's contact list (for WhatsApp-like functionality)
class Contact(me.Document):
    user = me.ReferenceField(User, required=True)
    contact = me.ReferenceField(User, required=True)
    is_blocked = me.BooleanField(default=False)

    def block_contact(self):
        self.is_blocked = True
        self.save()

    def unblock_contact(self):
        self.is_blocked = False
        self.save()
