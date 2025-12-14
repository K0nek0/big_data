import os
import sqlite3


class Config:
    # Настройки краулера
    START_URLS = [
        "https://en.wikipedia.org/wiki/Data_science",
        "https://en.wikipedia.org/wiki/Machine_learning",
        "https://en.wikipedia.org/wiki/Artificial_intelligence",
        "https://en.wikipedia.org/wiki/Web_crawler",
        "https://en.wikipedia.org/wiki/PageRank"
    ]
    
    MAX_PAGES = 30
    MAX_DEPTH = 2
    USER_AGENT = "MiniSearchEngineBot/1.0"
    
    # Настройки базы данных
    DB_PATH = "data/search_engine.db"
    
    CREATE_TABLES_SQL = """
    CREATE TABLE IF NOT EXISTS documents (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        url TEXT UNIQUE NOT NULL,
        title TEXT,
        content TEXT,
        pagerank REAL DEFAULT 1.0,
        indexed BOOLEAN DEFAULT FALSE
    );
    
    CREATE TABLE IF NOT EXISTS words (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        word TEXT UNIQUE NOT NULL
    );
    
    CREATE TABLE IF NOT EXISTS document_words (
        doc_id INTEGER,
        word_id INTEGER,
        tf REAL,
        positions TEXT,
        FOREIGN KEY (doc_id) REFERENCES documents(id),
        FOREIGN KEY (word_id) REFERENCES words(id),
        PRIMARY KEY (doc_id, word_id)
    );
    
    CREATE TABLE IF NOT EXISTS links (
        source_id INTEGER,
        target_id INTEGER,
        FOREIGN KEY (source_id) REFERENCES documents(id),
        FOREIGN KEY (target_id) REFERENCES documents(id),
        PRIMARY KEY (source_id, target_id)
    );
    
    CREATE TABLE IF NOT EXISTS word_stats (
        word_id INTEGER PRIMARY KEY,
        df INTEGER DEFAULT 1,
        FOREIGN KEY (word_id) REFERENCES words(id)
    );
    """
    
    # Настройки PageRank
    DAMPING_FACTOR = 0.85
    TOLERANCE = 1e-6
    MAX_ITERATIONS = 100

    # Настройки Pregel
    NUM_WORKERS = 4
    MAX_SUPERSTEPS = 30
    
    # Настройки поиска
    STOP_WORDS = {
        'a', 'an', 'the', 'and', 'or', 'but', 'in', 'on', 'at', 'to', 'for',
        'of', 'with', 'by', 'is', 'are', 'was', 'were', 'be', 'been', 'being',
        'have', 'has', 'had', 'do', 'does', 'did', 'will', 'would', 'shall',
        'should', 'may', 'might', 'must', 'can', 'could'
    }
    
    @classmethod
    def init_database(cls):
        """Инициализация базы данных"""
        os.makedirs(os.path.dirname(cls.DB_PATH), exist_ok=True)
        conn = sqlite3.connect(cls.DB_PATH)
        cursor = conn.cursor()
        
        # Проверяем существование таблиц
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        existing_tables = [row[0] for row in cursor.fetchall()]
        
        # Создаем таблицы, если их нет
        if 'documents' not in existing_tables:
            cursor.executescript(cls.CREATE_TABLES_SQL)
            conn.commit()
            print("База данных инициализирована")
        else:
            print("База данных уже существует")
        
        conn.close()
