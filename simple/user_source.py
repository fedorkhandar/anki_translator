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
class UserSource(Base):
    id: int = None
    body: str = None
    filename: str = None
    created_at: datetime.datetime = None
    hash: str = None
    parent_id: int = None
    folder: str = settings.USER_SOURCE_DIR

    # def __repr__(self):
    #     return f"filename={self.filename}, id={self.id}, len(body)={len(self.body)}"
    
    # @classmethod
    # async def create_folders(cls) -> Self:
    #     if not os.path.exists(settings.USER_SOURCE_DIR):
    #         os.makedirs(settings.USER_SOURCE_DIR)
    
    @classmethod
    async def get_parents(cls, session: aiosqlite.Connection) -> List[str]:
        """Parent is a filename fromwhere to get the source"""
        return [
            f"{cls.folder}/{x}" 
            for x in os.listdir(cls.folder)
        ]
    
    @classmethod
    async def from_parent(cls, parent: str, session: aiosqlite.Connection=None) -> Self:
        """read the file and return the source instance"""
        with codecs.open(parent, "r", "utf-8") as fin:
            body = fin.read().strip()
            body_hash = calc_hash(body)
            return cls(
                body=body,
                hash=body_hash,
                filename=parent,
                parent_id=int(parent.split("/")[-1].replace('.txt',''))
            )
    
    @classmethod
    async def save(cls, session: aiosqlite.Connection, sources: List[Self]) -> None:
        """
        Insert new (not alreasy existing) sources into the database
        """
        query = '''
        INSERT INTO user_source (body, hash)
        VALUES (?, ?)
        ON CONFLICT(hash) DO UPDATE SET body = excluded.body
        RETURNING id;
        '''

        sp = []
        for s in sources:
            returning_id = await db.insert_query_returning(
                session, 
                query, 
                (s.body, s.hash)
            )
            sp.append((s.parent_id, returning_id))
            
        query2 = '''INSERT INTO cleaned_source_m2m_user_source 
        (cleaned_source_id_fk, user_source_id_fk) VALUES (?, ?);'''              
        for p in sp:
            try:
                await db.insert_query(
                    session, 
                    query2, 
                    (p[0], p[1])
                )   
            except aiosqlite.IntegrityError:
                pass