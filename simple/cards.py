import asyncio
import aiohttp
from dataclasses import dataclass
from deep_translator import GoogleTranslator
import multiprocessing
from base import Base
from typing import List, Self, Dict, Tuple, Any, TypeVar, Union, Optional
import db
import aiosqlite
import datetime
import os
from config import settings
import codecs
from base import Base
from utils import calc_hash, save_files_to_folder
from config import settings
from dataclasses import dataclass
import orjson


W = TypeVar("W")  
JsonType = Union[None, int, str, bool, List[W], Dict[str, W]]


ANKI_URL = 'http://127.0.0.1:8765'

@dataclass
class PubNote:
    deck_name: str
    front: str
    back: str

    async def send(self):
        params = {
            "note":{
                "deckName":self.deck_name,
                "fields":{
                    "Back":self.back,
                    "Front":self.front
                },
                "modelName":"Basic"
            }
        }
        requestJson = {'action': 'addNote', 'params': params, 'version': 6}

        async with aiohttp.ClientSession() as session:
            async with session.post(ANKI_URL, json=requestJson) as resp:
                print(await resp.text())

async def create_note(data: Dict[str, Any]):
    # TODO: Pydantic scheme for input validation
    a = PubNote(
        data['deck_name'], 
        data['front'], 
        data['back']
    )
    await a.send()


@dataclass
class AnkiDefinition:
    definition: str
    example: str
    synonyms: List[str]
    antonyms: List[str]

@dataclass
class AnkiMeaning:
    antonyms: List[str]
    definitions: AnkiDefinition
    part_of_speech: str
    synonyms: List[str]

def highlight(whole_definition: str, word: str) -> str:
    return whole_definition.replace(word, f"<b>{word}</b>")

def clear_definition(whole_definition: str, word: str) -> str:
    return whole_definition.replace(word, "***")

POS_COLORS = {
    "noun": "#0000ff", 
    "verb": "#00ff00", 
    "adjective": "#ff0000", 
    "adverb":"#ff00ff", 
    "pronoun": "#00ffff", 
    "preposition": "#00ffff", 
    "conjunction": "#00ffff", 
    "interjection": "#00ffff", 
    "article": "#00ffff", 
    "determiner": "#00ffff", 
    "exclamation": "#00ffff"
}
@dataclass
class AnkiNote:
    deck_name: str
    word: str
    phonetic: str
    meanings: List[AnkiMeaning]
         
    @classmethod
    def from_json(cls, data: JsonType) -> Optional[Self]: 
        # TODO: pydantic scheme for input validation

        def extract_word(data: JsonType) -> Tuple[str, str, str, List[AnkiMeaning]]:
            deck_name = data.get("deck_name", "OOP")
            word = data.get("word", "")
            phonetic = data.get("phonetic", "").replace("//","")
            meanings_json = data.get("meanings", [])
            meanings = []

            for meaning in meanings_json:
                part_of_speech = meaning.get("partOfSpeech","")
                antonyms = meaning.get("antonyms", [])
                synonyms = meaning.get("synonyms", [])
                definitions = []
                definitions_list = meaning.get("definitions", [])
                for u in definitions_list:
                    definition = u.get("definition", "")
                    example = u.get("example", "")
                    synonyms = u.get("synonyms", [])
                    antonyms = u.get("antonyms", [])
                    definitions.append(AnkiDefinition(definition, example, synonyms, antonyms))
                meanings.append(AnkiMeaning(antonyms, definitions, part_of_speech, synonyms))  

            return deck_name, word, phonetic, meanings
        
        if data is not None:
            deck_name, word, phonetic, meanings = extract_word(data)
            return cls(deck_name, word, phonetic, meanings)
        else:
            return None
        
    def htmled_front(self):
        # TODO: templates representation and separate storing

        result = ''
        j = 0
        for meaning in self.meanings:
            result += '<div style="margin:10px;">'
            for u in meaning.definitions:
                pos_color = POS_COLORS.get(meaning.part_of_speech, "#000000")
                result += f'{j}. <span style="color:{pos_color};font-size: 50%;">{meaning.part_of_speech}</span> {clear_definition(u.definition, self.word)}<br>'
                j += 1
                
            result += '</div>'
        return result
    
    def htmled_back(self):
        # TODO: templates representation and separate storing
        result = f'<div style="margin:10px;"><b>{self.word}</b><br>{self.phonetic}</div>'
        j = 0
        for meaning in self.meanings:
            result += '<div style="margin:10px;">'
            for u in meaning.definitions:
                pos_color = POS_COLORS.get(meaning.part_of_speech, "#000000")
                result += f'{j}. <span style="color:{pos_color};font-size: 50%;">{meaning.part_of_speech}</span> {highlight(u.definition, self.word)}<br>'
                j += 1
                if u.example: 
                    t = highlight(u.example, self.word)
                    result += f'<span style="color: #0000ff;font-style: italic;">{highlight(u.example, self.word)}</span><br>'
                if u.synonyms: result += f'<span style="color: #009900;font-style: italic;">{u.synonyms}</span><br>'
                if u.antonyms: result += f'<span style="color: #990000;font-style: italic;">{u.antonyms}</span><br>'
            result += '</div>'
        return result

    def to_dict(self):
        return {
            "deck_name": self.deck_name,
            "front": self.htmled_front(),
            "back": self.htmled_back()
        } 

@dataclass
class Card(Base):
    id: int = None
    body: str = None
    created_at: datetime.datetime = None
    word_phrase_id_fk: int = None
    folder: str = settings.TRANSLATED_SOURCE_DIR

    @classmethod
    async def get_parents(cls, session: aiosqlite.Connection) -> List[str]:
        """Parent is a db record"""
        query = "SELECT id, interpretation FROM word_phrase WHERE to_card = 0 AND interpretation IS NOT NULL;"
        rows = await db.fetch_query(session, query)
        
        result = []
        for r in rows:
            result.extend([
                (r[0], AnkiNote.from_json(w))
                for w in orjson.loads(r[1])
            ])
        return result
    
        
    
    @classmethod
    async def from_parent(
        cls, parent: str,
        session: aiosqlite.Connection
    ) -> List[Self]:
        word_phrase_id_fk = parent[0]
        body = orjson.dumps(parent[1].to_dict())
        return cls(
            body=body,
            word_phrase_id_fk=word_phrase_id_fk
        )
    
    
    @classmethod
    async def save(cls, session: aiosqlite.Connection, sources: List[Self]) -> None:
        """
        Insert new (not alreasy existing) sources into the database
        """

        query1 = '''
        INSERT INTO card (body, word_phrase_id_fk)
        VALUES (?, ?)
        ON CONFLICT(body) DO UPDATE SET word_phrase_id_fk = excluded.word_phrase_id_fk
        RETURNING id;
        '''
        tasks = []
        processed_ids = []
        for s in sources:
            returning_id = await db.insert_query_returning(
                session, 
                query1, 
                (s.body, s.word_phrase_id_fk)
            )
            processed_ids.append(s.word_phrase_id_fk)
            tasks.append(
                asyncio.create_task(
                    create_note(
                        orjson.loads(s.body)
                    )
                )
            )
        await asyncio.gather(*tasks)

        # print(f"processed_ids={processed_ids}")
        query2 = "UPDATE word_phrase SET to_card = 1 WHERE id IN ({})".format(
            ",".join("?" * len(processed_ids))
        )
        await db.execute_query(session, query2, processed_ids)
