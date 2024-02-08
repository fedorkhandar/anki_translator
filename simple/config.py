class Settings:
    DB_NAME = "data/atdb.db"
    
    SOURCE_DIR = "sources"
    DELETE_SOURCES = False
    
    CLEANED_SOURCE_DIR = "cleaned_sources"
    USER_SOURCE_DIR = "user_sources"
    TRANSLATED_SOURCE_DIR = "translated_sources"
    SENTENCE_DIR = "sentences"
    WORD_PHRASE_DIR = "user_words_and_phrases"
    TRANSLATION_PROCESSES_COUNT = 4

settings = Settings()