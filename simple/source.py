from dataclasses import dataclass
from base import Base
from typing import List, Self
import db
import aiosqlite
import datetime
import os
from config import settings
import codecs
from base import Base
from utils import calc_hash
from config import settings

    
@dataclass
class Source(Base):
    id: int = None
    body: str = None
    filename: str = None
    created_at: datetime.datetime = None
    hash: str = None
    folder: str = settings.SOURCE_DIR
    
    # @classmethod
    # async def create_folders(cls) -> Self:
    #     if not os.path.exists(settings.SOURCE_DIR):
    #         os.makedirs(settings.SOURCE_DIR)
    
    @classmethod
    async def get_parents(cls, session: aiosqlite.Connection) -> List[str]:
        """Parent is a filename fromwhere to get the source"""
        return [
            f"{settings.SOURCE_DIR}/{x}" 
            for x in os.listdir(settings.SOURCE_DIR)
        ]
    
    @classmethod
    async def from_parent(cls, parent: str, session: aiosqlite.Connection=None) -> Self:
        """read the file and return the source instance"""
        with codecs.open(parent, "r", "utf-8") as fin:
            body = fin.read().strip()
            body_hash = calc_hash(body)
            return cls(
                body=body,
                filename=parent,
                hash=body_hash
            )
 
    @classmethod
    async def save(
        cls, 
        session: 
        aiosqlite.Connection, 
        sources: List[Self]
    ) -> None:
        """
        Insert new (not alreasy existing) sources into the database
        """
        query = '''
        INSERT INTO source (body, filename, hash)
        VALUES (?, ?, ?)
        ON CONFLICT(hash) DO UPDATE SET filename = excluded.filename
        RETURNING id;
        '''

        for s in sources:
            returning_id = await db.insert_query_returning(
                session,
                query,
                (s.body, s.filename, s.hash)
            )
            # print(f"File '{s.filename}' [{returning_id}]")
            
            if settings.DELETE_SOURCES:
                os.remove(s.filename)
                print(f"File '{s.filename}' was deleted")
