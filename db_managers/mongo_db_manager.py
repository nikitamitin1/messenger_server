from motor.motor_asyncio import AsyncIOMotorClient
from pymongo.errors import DuplicateKeyError
from .base_db_manager import DBManager

class MongoDBManager(DBManager):
    def __init__(self, host: str, port: int, database_name: str):
        super().__init__(host, port, database_name)
        self.client = None
        self.db = None

    async def connect(self):

        try:
            self.client = AsyncIOMotorClient(f"mongodb://{self.host}:{self.port}/")
            self.db = self.client[self.database_name]
            print(f"Connected to MongoDB database: {self.database_name}")
        except Exception as e:
            print(f"Error connecting to MongoDB: {e}")

    async def close(self):

        if self.client:
            self.client.close()
            print("MongoDB connection closed.")
        else:
            print("No active MongoDB connection to close.")

    # async def execute_query(self, query: dict, collection_name: str, **kwargs):
    #
    #     if self.db:
    #         collection = self.db[collection_name]
    #         cursor = collection.find(query)
    #         result = await cursor.to_list(length=None)
    #         return result
    #     else:
    #         print("Not connected to any database.")
    #         return None

    async def add_document(self, collection_name: str, document: dict, **kwargs):
        try:
            collection = self.db[collection_name]
            insert_result = await collection.insert_one(document, **kwargs)
            print(f"Document added with ID: {insert_result.inserted_id}")
            return insert_result
        except DuplicateKeyError:
            print("Duplicate key error. Document already exists.")
            return None
        except Exception as e:
            print(f"Error adding document: {e}")
            return None
    async def add_many_documents(self, collection_name: str, documents: list, **kwargs):
        try:
            collection = self.db[collection_name]
            insert_result = await collection.insert_many(documents, **kwargs)
            print(f"Documents added with IDs: {insert_result.inserted_ids}")
            return insert_result
        except DuplicateKeyError:
            print("Duplicate key error. Document already exists.")
            return None
        except Exception as e:
            print(f"Error adding document: {e}")
            return None

    async def update_document(self, collection_name: str, query: dict, update: dict):
        """
        :param collection_name: Имя коллекции для обновления.
        :param query: Словарь для поиска документов.
        :param update: Словарь с данными для обновления.
        :return: Количество обновлённых документов.
        """
        try:
            collection = self.db[collection_name]
            update_result = await collection.update_many(query, {"$set": update})
            print(
                f"Matched {update_result.matched_count} document(s), modified {update_result.modified_count} document(s)."
            )
            return update_result.modified_count
        except Exception as e:
            print(f"Error updating document: {e}")
            return 0

    async def update_many_documents(self, collection_name: str, query: dict, update: dict):
        try:
            collection = self.db[collection_name]
            update_result = await collection.update_many(query, {"$set": update})
            print(
                f"Matched {update_result.matched_count} document(s), modified {update_result.modified_count} document(s)."
            )
            return update_result.modified_count
        except Exception as e:
            print(f"Error updating document: {e}")
            return 0

    async def read_documents(self, collection_name: str, query: dict = {}, **kwargs):
        """
        :param collection_name: Имя коллекции для чтения.
        :param query: Словарь с параметрами запроса.
        :return: Список документов, удовлетворяющих запросу. ObjectID as a str
        """
        try:
            collection = self.db[collection_name]
            cursor = collection.find(query)

            if 'sort' in kwargs:
                cursor = cursor.sort(kwargs['sort'])
            if 'skip' in kwargs:
                cursor = cursor.skip(kwargs['skip'])
            if 'limit' in kwargs:
                cursor = cursor.limit(kwargs['limit'])

            length = kwargs.get('limit', None)
            result = await cursor.to_list(length=length)
            return result
        except Exception as e:
            print(f"Error reading documents: {e}")
            return []