from abc import ABC, abstractmethod
import json


class CacheManager(ABC):

    @abstractmethod
    async def load_data(self, key: str):
        pass

    @abstractmethod
    async def get_data(self, key: str):
        pass

    @abstractmethod
    async def set_data(self, key: str, data: dict):
        pass

    @abstractmethod
    async def delete_data(self, key: str):
        pass
