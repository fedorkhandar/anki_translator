import asyncio
from dataclasses import dataclass
import aiosqlite
from typing import List, Self, Union
from typing import TypeVar
from abc import abstractmethod, ABCMeta
import os
import time


T = TypeVar("T")
P = TypeVar("P")


def abstractclassmethod(func):
    """Wraps a classmethod to make it abstract."""
    return classmethod(abstractmethod(func))


@dataclass
class Base(metaclass=ABCMeta):
    folder: str = None

    @abstractclassmethod
    async def from_parent(cls, parent: P, session: aiosqlite.Connection = None) -> Self:
        """Abstract classmethod to get an instance from Parent"""
        pass

    @abstractclassmethod
    async def get_parents(cls, session: aiosqlite.Connection) -> List[P]:
        """Abstract classmethod to get Parents from data"""
        pass

    @abstractclassmethod
    async def save(
        cls, session: aiosqlite.Connection, sources: Union[List[Self], List[List[Self]]]
    ) -> None:
        """Abstract classmethod to save instances (to db, to FS, ...)"""
        pass

    @classmethod
    async def create_folders(cls) -> Self:
        if cls.folder is not None:
            if not os.path.exists(cls.folder):
                os.makedirs(cls.folder)


class ResourceSet:
    session: aiosqlite.Connection
    items: List[T] = None

    def __init__(self, instance: T, session: aiosqlite.Connection):
        self.instance = instance
        self.session = session

    async def do_work(self):
        """
        1. Gets parents
        2. Construct items from parents
        3. Save items
        """
        start = time.time()
        # print("mamma")
        print(self.instance.__class__.__name__, end=": ")
        await self.instance.create_folders()

        parents = await self.instance.get_parents(self.session)
        # print(f"parents={[type(p) for p in parents]}")

        tasks = [self.instance.from_parent(p, self.session) for p in parents]
        self.items = await asyncio.gather(*tasks)
        # print("self.items=", self.items)

        await self.instance.save(self.session, self.items)
        print(f"{len(self.items)} items, {time.time() - start:.2f} sec")
