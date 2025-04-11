import mongoengine as me
import datetime
from models.models import User, Chat, Message, Media, Notification, Contact
from pymongo import MongoClient
from pymongo.errors import OperationFailure


# Подключение к базе данных MongoDB с авторизацией
def connect_to_db():
    # Подключаемся к MongoDB
    client = MongoClient('mongodb://admin:admin@localhost:27017/admin')

    client.drop_database('whatsapp_clone')
    # Пытаемся подключиться к базе данных 'whatsapp_clone'
    db = client['whatsapp_clone']

    # Проверка, существует ли база данных
    if 'whatsapp_clone' not in client.list_database_names():
        print("Database 'whatsapp_clone' does not exist, creating...")
        # База данных будет создана при первом сохранении

    # Создаем пользователя 'admin' с правами администратора (если не существует)
    try:
        # Попробуем создать пользователя 'admin'
        db.command("createUser", "admin", pwd="admin", roles=[{"role": "root", "db": "admin"}])
        print("User 'admin' created with root access.")
    except OperationFailure:
        # Пользователь уже существует
        print("User 'admin' already exists.")

    # Подключение через mongoengine
    me.connect('whatsapp_clone', host='localhost', port=27017, username='admin', password='admin',
               authentication_source='admin')


# Функция для сидинга данных
def seed_data():
    # Создание пользователей
    user1 = User(name="John Doe", email="john@example.com", phone_number="1234567890")
    user1.save()

    user2 = User(name="Jane Smith", email="jane@example.com", phone_number="0987654321")
    user2.save()

    user3 = User(name="Alice Johnson", email="alice@example.com", phone_number="1122334455")
    user3.save()

    user4 = User(name="Bob Brown", email="bob@example.com", phone_number="9988776655")
    user4.save()

    # Создание чатов
    chat1 = Chat(name="Group Chat 1", participants=[user1.id, user2.id], is_group=True)
    chat1.save()

    chat2 = Chat(name="Group Chat 2", participants=[user2.id, user3.id, user4.id], is_group=True)
    chat2.save()

    chat3 = Chat(name="Individual Chat", participants=[user1.id, user4.id], is_group=False)
    chat3.save()

    # Обновление поля chats для пользователей
    # Обновляем каждого пользователя, добавляя новый чат в его список чатов
    user1.chats.append(chat1.id)
    user1.chats.append(chat3.id)
    user1.save()

    user2.chats.append(chat1.id)
    user2.chats.append(chat2.id)
    user2.save()

    user3.chats.append(chat2.id)
    user3.save()

    user4.chats.append(chat2.id)
    user4.chats.append(chat3.id)
    user4.save()

    # Создание сообщений
    message1 = Message(sender=user1, chat=chat1, text="Hello, how are you?")
    message1.save()

    message2 = Message(sender=user2, chat=chat1, text="I'm fine, thanks! How about you?")
    message2.save()

    message3 = Message(sender=user3, chat=chat2, text="Hey, everyone!")
    message3.save()

    message4 = Message(sender=user4, chat=chat2, text="Hello Alice!")
    message4.save()

    chat1.last_message = message1
    chat1.save()

    chat2.last_message = message3
    chat2.save()

    chat3.last_message = message4
    chat3.save()

    # Создание медиа
    media = Media(file_type="image", file_url="https://example.com/image.jpg", sender=user1)
    media.save()

    # Создание уведомлений
    notification1 = Notification(recipient=user2, message="You have a new message", type="message")
    notification1.save()

    notification2 = Notification(recipient=user3, message="You have a new message", type="message")
    notification2.save()

    # Добавление контактов
    contact1 = Contact(user=user1, contact=user2, is_blocked=False)
    contact1.save()

    contact2 = Contact(user=user2, contact=user3, is_blocked=False)
    contact2.save()

    contact3 = Contact(user=user3, contact=user4, is_blocked=False)
    contact3.save()

    print("Data seeded successfully!")

# Вызов функции сидинга
connect_to_db()
seed_data()

