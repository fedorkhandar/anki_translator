import asyncio
from dataclasses import dataclass
from base import Base
from typing import List, Self, Dict, Any, Union, TypeVar
import db
import aiosqlite
import datetime
import os
from config import settings
import codecs
from base import Base
from utils import calc_hash
from config import settings
import aiohttp
import time
import orjson

    
W = TypeVar("W")  
JsonType = Union[None, int, str, bool, List[W], Dict[str, W]]


URL = "https://api.dictionaryapi.dev/api/v2/entries/en/{}"
MAX_CONCURRENT_REQUESTS = 5


async def get_interpetation(body):
    return (body, f"interpretation of {body}")
    # TODO: get interpretation from dictionary


async def get_word(session, word: str) -> JsonType:
    url = URL.format(word)
    print(f"url={url}")
    async with session.get(url) as resp:
        u = await resp.json()
        # if resp.status == 200:
        #     print(f"response json = {u}")
        return {"word":word, "resp":u} if resp.status == 200 else {"word":word, "resp":None}


async def interpret(words: List[str]) -> List[JsonType]:
    # TODO: Pydantic scheme for output validation

    result = []
    n = len(words)
    async with aiohttp.ClientSession() as asession:
        for i in range(0, n, MAX_CONCURRENT_REQUESTS):
            tasks = [
                asyncio.create_task(get_word(asession, word)) 
                for word in words[i:i+MAX_CONCURRENT_REQUESTS]
            ]
            result = result + await asyncio.gather(*tasks)

    return result


async def join_result(result: List[JsonType]) -> List[JsonType]:
    result2 = []
    for r in result:
        if isinstance(r, list):
            for u in r:
                result2.append(u)
        else:
            result2.append(r)
    return result2



async def remove_existing(
    session: aiosqlite.Connection, 
    words: List[str]
) -> List[str]:
    query = "SELECT COUNT(*) FROM word_phrase WHERE body = ?;"
    words2 = []
    for word in words:
        check_result = await db.count_query(session, query, (word,)) 
        # print(f"word={word}, check_result={check_result}")
        if check_result is None or check_result[0] == 0:
            words2.append(word)

    return words2

@dataclass
class WordPhrase(Base):
    id: int = None
    body: str = None
    interpretation: str = None
    created_at: datetime.datetime = None
    parent_id: int = None
    folder: str = settings.WORD_PHRASE_DIR

    
    @classmethod
    async def get_parents(
        cls,
        session: aiosqlite.Connection
    ) -> List[str]:
        """
        1. parents = await self.instance.get_parents(self.session)
        Parents are the files in 
        settings.WORD_PHRASE_DIR = "user_words_and_phrases

        2. tasks = [self.instance.from_parent(p, self.session) for p in parents]
        self.items = await asyncio.gather(*tasks)

        from_parent() reads the file -> content: the list of words
        for every word in content -> get interpretation 
            -> List of Tuple[word, interpretation]
        self.items -> List of List of Tuple[word, interpretation]

        3. await self.instance.save(self.session, self.items)
        convert self.items -> List of Tuple[word, interpretation]
        save to db

        """
        return [
            f"{cls.folder}/{x}" 
            for x in os.listdir(cls.folder)
        ]
    
    @classmethod
    async def from_parent(
        cls,
        parent: str,
        session: aiosqlite.Connection
    ) -> List[Self]:
        """
        from_parent() reads the file -> content: the list of words
        for every word in content -> get interpretation 
            -> List of Tuple[word, interpretation]
        self.items -> List of List of Tuple[word, interpretation]

        Interpretation is a dictionary with the following keys:
        - word: str
        - ...

        """
        returned_data = []
        with codecs.open(parent, "r", "utf-8") as fin:
            words = [word.strip() for word in fin.readlines()]
        
        print(f"words={words}")
        words = await remove_existing(session, words)
        print(f"words2={words}")
        returned_data = []
        if len(words)>0:
            result = await interpret(words)
            result = await join_result(result)
            
            for r in result:
                returned_data.append(
                    WordPhrase(
                        body=r["word"],
                        # TODO: zip json in db
                        interpretation=orjson.dumps(r["resp"]) if r["resp"] is not None else None,
                        parent_id=parent.split('/')[-1].replace(".txt", "").split("_")[-1]
                        #parent_id = translated_source_id
                    )
                )

        return returned_data

    @classmethod
    async def save(cls,
                   session: aiosqlite.Connection,
                   sources: List[Self]
                   ) -> None:
        """
        Insert new (not alreasy existing) sources into the database
        """
        sources2 = []
        for s in sources:
            sources2.extend(s)
        sources = sources2

        query = '''
        INSERT INTO word_phrase (body, interpretation)
        VALUES (?, ?)
        ON CONFLICT(body) DO
        UPDATE SET interpretation = excluded.interpretation
        RETURNING id;
        '''
        
        # print(f"len(sources)={len(sources)}, type(sources)={type(sources)}")

        with codecs.open(f"waph.json", "w", "utf-8") as fout:
            print(sources, file=fout)    
        # sp = []
        for s in sources:
            # print(f"s={s.body}")
            returning_id = await db.insert_query_returning(
                session,
                query,
                (s.body, s.interpretation)
            )
            # sp.append((s.parent_id, returning_id))
            
        # query2 = '''INSERT INTO sentence_m2m_word_phrase
        # (sentence_id_fk, word_phrase_id_fk) VALUES (?, ?);'''              
        # for p in sp:
        #     try:
        #         await db.insert_query(
        #             session, 
        #             query2, 
        #             (p[0], p[1])
        #         )   
        #     except aiosqlite.IntegrityError:
        #         pass