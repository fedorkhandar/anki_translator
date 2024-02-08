import asyncio
from dataclasses import dataclass
from deep_translator import GoogleTranslator
import multiprocessing
from base import Base
from typing import List, Self
import db
import aiosqlite
import datetime
import os
from config import settings
import codecs
from base import Base
from utils import calc_hash, save_files_to_folder
from config import settings
from typing import List, Tuple, Any


async def translate_one_line(
    line: str,
    session: aiosqlite.Connection,
    translated_source_id: int
) -> str:
    """make translation from the source"""
    # TODO: check if line exists in db.sentences
    query = "SELECT translation FROM sentence WHERE body = ?;"
    data = (line,)

    # print(f"SELECT translation FROM sentence WHERE body = '{line}'")
    rows = await db.fetch_query(session, query, data)
    if rows is not None and len(rows)>0:
        return f"{line}\t{rows[0][0]}"
    # print(f"rows={rows}")
    # TODO: do not translate sentences that are already in db

    translation = GoogleTranslator(source='english', target='russian').translate(line).strip()

    # TODO: push the line into db.sentences
    query2 = "INSERT INTO sentence (body, translation) VALUES (?, ?);"
    data2 = (line, translation)
    sentence_id = await db.insert_query_returning(session, query2, data2)

    # query3 = "INSERT INTO translated_source_m2m_sentence (translated_source_id_fk, sentence_id_fk) VALUES (?, ?);"
    # data3 = (translated_source_id, sentence_id)
    # await db.insert_query(session, query3, data3)

    res = f"{line}\t{translation}"
    # print(translation)
    return res 
    #TODO : it seems it doesn't work asyncronously

async def make_translation(
    body: str,
    session: aiosqlite.Connection,
    translated_source_id: int
) -> str:
    """make translation from the source"""

    lines = [line.strip() for line in body.split("\n") if line != "" and line != "\n"]
    tasks = [asyncio.create_task(translate_one_line(line, session, translated_source_id)) for line in lines]
    translated_lines = await asyncio.gather(*tasks)
    
    return "\n".join(translated_lines)

    # TODO: implement multiprocessor translation


@dataclass
class TranslatedSource(Base):
    id: int = None
    body: str = None
    filename: str = None
    created_at: datetime.datetime = None
    hash: str = None
    parent_id: int = None
    folder: str = settings.TRANSLATED_SOURCE_DIR

    def __repr__(self):
        return f"filename={self.filename}, id={self.id}, len(body)={len(self.body)}"
    
    # @classmethod
    # async def create_folders(cls) -> Self:
    #     if not os.path.exists(settings.TRANSLATED_SOURCE_DIR):
    #         os.makedirs(settings.TRANSLATED_SOURCE_DIR)
    
    @classmethod
    async def get_parents(cls, session: aiosqlite.Connection) -> List[str]:
        """Parent is a filename fromwhere to get the source"""
        return [
            f"{settings.USER_SOURCE_DIR}/{x}" 
            for x in os.listdir(settings.USER_SOURCE_DIR)
        ]
    
    @classmethod
    async def from_parent(
        cls, parent: str,
        session: aiosqlite.Connection
    ) -> Self:
        """read the file and return the source instance"""
        with codecs.open(parent, "r", "utf-8") as fin:
            translated_source_id = parent.replace('.txt','').split("_")[-1]
            body = await make_translation(fin.read().strip(), session, translated_source_id)
            body_hash = calc_hash(body)
            return cls(
                body=body,
                hash=body_hash,
                filename=f"{parent}_translated.txt",
                parent_id=int(parent.split("/")[-1].replace('.txt',''))
            )
    
    @classmethod
    async def save(cls, session: aiosqlite.Connection, sources: List[Self]) -> None:
        """
        Insert new (not alreasy existing) sources into the database
        """
        # query = "INSERT INTO translated_source (body, is_processed) VALUES (?, ?);"

        query = '''
        INSERT INTO translated_source (body, hash)
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
            
        query2 = '''INSERT INTO user_source_m2m_translated_source 
        (user_source_id_fk, translated_source_id_fk) VALUES (?, ?);'''              
        for p in sp:
            try:
                await db.insert_query(
                    session, 
                    query2, 
                    (p[0], p[1])
                )     
            except aiosqlite.IntegrityError:
                pass

        # query3 = "SELECT * FROM translated_source;"
        # rows = await db.fetch_query(session, query3)
        # new_files = [(f"{row[0]}.txt", row[1]) for row in rows]
        # await save_files_to_folder(new_files, settings.TRANSLATED_SOURCE_DIR)

        query3 = '''
        SELECT usm2mts.user_source_id_fk, ts.id, ts.body 
        FROM translated_source AS ts 
        JOIN user_source_m2m_translated_source AS usm2mts 
        ON ts.id = usm2mts.translated_source_id_fk;
        '''
        rows = await db.fetch_query(session, query3)
        new_files = [(f"{row[0]}_{row[1]}.txt", row[2]) for row in rows]
        await save_files_to_folder(new_files, settings.TRANSLATED_SOURCE_DIR)
