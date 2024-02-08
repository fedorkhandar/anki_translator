import aiosqlite
import asyncio  
import db
from config import settings
from base import ResourceSet
from source import Source
from cleaned_source import CleanedSource
from user_source import UserSource
from translated_source import TranslatedSource
from word_phrase import WordPhrase
import shutil
import os
from cards import Card


TABLES = [
    'source', 
    'cleaned_source', 
    'source_m2m_cleaned_source', 
    'user_source',
    'cleaned_source_m2m_user_source',
    'translated_source',
    'user_source_m2m_translated_source',
    'sentence',
    # 'translated_source_m2m_sentence',
]

COLUMNS = [
    "source",
    "cleaned_source",
    "user_source",
    "translated_source",
    "word_phrase",
    "sentence",
]

FOLDERS = [
    settings.CLEANED_SOURCE_DIR,
    settings.USER_SOURCE_DIR,
    settings.TRANSLATED_SOURCE_DIR,
    settings.WORD_PHRASE_DIR
]

async def clear_db(session: aiosqlite.Connection):
    tasks = [db.truncate_table(session, t) for t in TABLES]
    await asyncio.gather(*tasks)
    query = "UPDATE sqlite_sequence SET seq = 0 WHERE name = ?;"
    for c in COLUMNS:
        await db.executemany(session, query, [(c,)])
    print("DB was cleared")
    for folder in FOLDERS:
        if os.path.exists(folder):
            shutil.rmtree(folder)
        os.makedirs(folder)
  

async def main(clear_db_flag: bool = False):
    session = await aiosqlite.connect(settings.DB_NAME)

    # clear db (for debug purposes)
    if clear_db_flag:
        await clear_db(session)

    # files in folder 'sources' -> records in table 'source'
    source_set = ResourceSet(Source(), session)
    await source_set.do_work()

    # records in table 'source' -> 
    # records in table 'cleaned_source' + files in folder 'cleaned_sources'
    cleaned_source_set = ResourceSet(CleanedSource(), session)
    await cleaned_source_set.do_work()

    # === USER MANUALLY PUTS FILES 
    # FROM 'cleaned_sources' TO 'user_sources' ===
    # print("Now process manually every file in 'cleaned_sources' and put it to 'user_sources'")

    # files in folder 'user_sources' -> records in table 'user_source'
    user_source_set = ResourceSet(UserSource(), session)
    await user_source_set.do_work()

    # records in table 'user_source' -> records in table 'translated_source' + files in folder 'translated_sources'
    translated_source_set = ResourceSet(TranslatedSource(), session)
    await translated_source_set.do_work()

    # === USER MANUALLY PUTS FILES FROM 'translated_sources' TO 'words_and_phrases' ===
    # files in folder 'words_and_phrases' -> records in table 'words_and_phrases'
    waph_set = ResourceSet(WordPhrase(), session)
    await waph_set.do_work()

    card_set = ResourceSet(Card(), session)
    await card_set.do_work()

    await session.close()

if __name__ == "__main__":
    asyncio.run(main(clear_db_flag=True))
    # asyncio.run(main())