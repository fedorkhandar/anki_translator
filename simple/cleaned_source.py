import codecs
from dataclasses import dataclass
from base import Base
from utils import calc_hash, save_files_to_folder
from typing import List, Self
import db
import aiosqlite
import datetime
from source import Source
import re
from config import settings


def clear(line: str) -> str:
    """clear line from the extra symbols"""
    line = line.strip()
    line= line.replace("\ufeff","")
    line = re.sub(r"<[/]*i>", "", line)
    line = re.sub(r"\[[0-9a-zA-Z!?.,:\"\-' ]*\]", "", line)
    
    zline = re.sub(r"[0-9][0-9:, \->]*", "", line)
    if zline.strip() == '':
        line = zline
        
    line = line.replace("♪","")
    while len(line)>0 and line[0] == '-':
        line = line[1:]
    while len(line)>0 and line[-1] == '-':
        line = line[:-1]
    
    line = line.replace('--',' -- ')
    line = line.strip()
    
    while line.find('  ')>=0:
        line = line.replace('  ', ' ')

    return line

def clear_comments(line: str) -> str:
    """delete comments from the line"""
    line = re.sub(r"<[/]*i>", "", line)
    line = re.sub(r"\[[0-9a-zA-Z!?.,:\"\-' ]*\]", "", line)
    return line

def hyphenations(line: str) -> bool:
    """delete extra hyphenations"""
    line = line.strip()
    if not line:
        return False
    return line[-1].isalnum() or line.endswith("...") or line.endswith("--") or line[-1]==','
    
def dedup(lines):
    """delete duplicates from the list of lines"""
    to_delete = set()
    n = len(lines)
    for i, x in enumerate(lines):
        for j in range(i+1, n):
            if lines[j] == x:
                to_delete.add(j)
                
    result = []
    for i, x in enumerate(lines):
        if not i in to_delete:
            result.append(x)
            
    return result
    
def clean_srt(body):
    lines = [line for line in body.split("\n") if line !=""]
    
    res_lines = []
    
    for line in lines:
        line = clear(line)
        if len(line)>0:
            if len(res_lines)>0 and hyphenations(res_lines[-1]):
                res_lines[-1] += " "+line
            else:
                res_lines.append(line)
    
    for i, line in enumerate(res_lines):
        while len(line)>0 and line[-1] in ".-!?":
            line = line[:-1]
        res_lines[i] = line
    res_lines = dedup(res_lines)

    for i, line in enumerate(res_lines):
        res_lines[i] = clear_comments(line)

    res_lines = [x for x in res_lines if x.strip()!='']
    return "\n".join(res_lines)


@dataclass
class CleanedSource(Base):

    body: str = None
    id: int = None
    created_at: datetime.datetime = None
    hash: str = None
    parent_id: int = None
    folder: str = settings.CLEANED_SOURCE_DIR

    # @classmethod
    # async def create_folders(cls) -> Self:
    #     if not os.path.exists(settings.CLEANED_SOURCE_DIR):
    #         os.makedirs(settings.CLEANED_SOURCE_DIR)

    @classmethod
    async def from_parent(cls, parent: Source, session: aiosqlite.Connection=None) -> Self:
        """create cleaned source from source"""
        cleaned_body = clean_srt(parent.body)
        
        return cls(
            body=cleaned_body,
            hash=calc_hash(cleaned_body),
            parent_id=parent.id
        )
    
    @classmethod
    async def get_parents(cls, session: aiosqlite.Connection) -> List[Source]:
        """Parent is a source from db which has no cleaned source"""
        
        query = '''WITH cte AS (SELECT DISTINCT source_id_fk FROM source_m2m_cleaned_source) 
        SELECT * FROM source WHERE id NOT IN (SELECT source_id_fk FROM cte);'''
        rows = await db.fetch_query(session, query)
        return [Source(id=row[0], body=row[1], filename=row[2]) for row in rows]
    
    @classmethod
    async def save(cls, session: aiosqlite.Connection, sources: List[Self]) -> None:
        """
        insert into db cleaned source if not already exists
        insert link source-cleaned source 
        """
        
        query1 = '''
        INSERT INTO cleaned_source (body, hash)
        VALUES (?, ?)
        ON CONFLICT(hash) DO UPDATE SET body = excluded.body
        RETURNING id;
        '''
        sp = []
        for s in sources:
            returning_id = await db.insert_query_returning(
                session, 
                query1, 
                (s.body, s.hash)
            )
            sp.append((s.parent_id, returning_id))

        query2 = '''INSERT INTO source_m2m_cleaned_source 
        (source_id_fk, cleaned_source_id_fk) VALUES (?, ?);'''              
        for p in sp:
            await db.insert_query(
                session, 
                query2, 
                (p[0], p[1])
            )
        
        query3 = "SELECT * FROM cleaned_source;"
        rows = await db.fetch_query(session, query3)
        new_files = [(f"{row[0]}.txt", row[1]) for row in rows]
        await save_files_to_folder(new_files, settings.CLEANED_SOURCE_DIR)

                