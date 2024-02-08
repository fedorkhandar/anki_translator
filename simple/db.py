import aiosqlite
from typing import List, Tuple, Any


async def execute_query(
    session: aiosqlite.Connection,
    query: str,
    data: Any = None
):
    if data is None:
        async with session.execute(query) as cursor:
            await session.commit()
    else:
        async with session.execute(query, data) as cursor:
            await session.commit()
            
async def truncate_table(
    session: aiosqlite.Connection, 
    table_name: str
):
    async with session.execute(f"DELETE FROM {table_name};") as cursor:
        await session.commit()
        
async def executemany(
    session: aiosqlite.Connection, 
    query: str,
    data: Any = None
):
    async with session.executemany(query, data) as cursor:
        await session.commit() 

async def count_query(
    session: aiosqlite.Connection, 
    query: str,
    data: Any = None
):
    if data is None:
        async with session.execute(query) as cursor:
            rows = await cursor.fetchone()
            return rows
    else:
        # print("db: HERE")
        try:
            async with session.execute(query, data) as cursor:
                rows = await cursor.fetchone()
                return rows
        except Exception as E:
            print(f"db: error: {E}")

async def fetch_query(
    session: aiosqlite.Connection, 
    query: str,
    data: Any = None
):
    # print(f"db: data={data}")
    # print(f"db: query={query}")
    if data is None:
        async with session.execute(query) as cursor:
            rows = await cursor.fetchall()
            return rows
    else:
        # print("db: HERE")
        try:
            async with session.execute(query, data) as cursor:
                rows = await cursor.fetchall()
                return rows
        except Exception as E:
            print(f"db: error: {E}")
        
async def insert_query_returning(
    session: aiosqlite.Connection, 
    query: str,
    data: list
):
    async with session.execute(query, data) as cursor:
        try:
            returning_id = await cursor.fetchone()
            await session.commit()
            if returning_id is None:
                return None
            return returning_id[0]
        except aiosqlite.OperationalError as e:
            print("exception: ", e)
            return None
        
async def insert_query(
    session: aiosqlite.Connection, 
    query: str,
    data: list
):
    async with session.execute(query, data) as cursor:
        try:
            await session.commit()
            return True
        except aiosqlite.OperationalError as e:
            print("exception: ", e)
            return False

