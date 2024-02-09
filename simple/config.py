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

    ANKI_URL = "http://127.0.0.1:8765"

    SYNONYM_STR = '<span style="color: #009900;font-style: italic;">{}</span><br>'
    ANTONYM_STR = '<span style="color: #990000;font-style: italic;">{}</span><br>'
    EXAMPLE_STR = '<span style="color: #0000ff;font-style: italic;">{}</span><br>'

settings = Settings()
