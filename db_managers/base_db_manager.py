from pymongo import MongoClient, UpdateOne
from pymongo.errors import DuplicateKeyError

class DBManager:
    def __init__(self, host: str, port: int, database_name: str):
        self.host = host
        self.port = port
        self.database_name = database_name
        self.connection = None

    def connect(self):
        """
        Establishes a connection to the database.
        This method should be implemented in subclasses.
        """
        raise NotImplementedError("Subclasses should implement this method.")

    def close(self):
        """
        Closes the database connection.
        """
        if self.connection:
            self.connection.close()
            print("Connection closed.")
        else:
            print("No active connection to close.")

    def execute_query(self, query: dict, collection_name: str):
        """
        Executes a query on the database.
        This method should be implemented in subclasses.
        """
        raise NotImplementedError("Subclasses should implement this method.")