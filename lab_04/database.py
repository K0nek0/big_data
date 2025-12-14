import sqlite3
import re
from typing import List, Dict, Tuple
from collections import defaultdict, Counter
from config import Config
import math


class DatabaseManager:
    def __init__(self):
        self.conn = sqlite3.connect(Config.DB_PATH)
        self.conn.row_factory = sqlite3.Row
    
    def close(self):
        self.conn.close()
    
    def tokenize(self, text: str) -> List[str]:
        """Токенизация текста"""
        # Приводим к нижнему регистру и разбиваем на слова
        words = re.findall(r'\b[a-z]{2,}\b', text.lower())
        # Убираем стоп-слова
        words = [w for w in words if w not in Config.STOP_WORDS]
        return words
    
    def build_index(self):
        """Построение инвертированного индекса"""
        cursor = self.conn.cursor()
        
        # Получаем все документы
        cursor.execute("SELECT id, content FROM documents WHERE indexed = FALSE")
        documents = cursor.fetchall()
        
        word_to_id = {}
        word_counter = Counter()
        document_vectors = defaultdict(dict)
        
        # Первый проход: подсчет TF и сбор статистики
        for doc_id, content in documents:
            words = self.tokenize(content)
            total_words = len(words)
            
            if total_words == 0:
                continue
            
            # Подсчитываем TF
            word_freq = Counter(words)
            for word, freq in word_freq.items():
                tf = freq / total_words
                
                # Сохраняем ID слова
                if word not in word_to_id:
                    cursor.execute("INSERT OR IGNORE INTO words (word) VALUES (?)", (word,))
                    cursor.execute("SELECT id FROM words WHERE word = ?", (word,))
                    word_id = cursor.fetchone()[0]
                    word_to_id[word] = word_id
                else:
                    word_id = word_to_id[word]
                
                document_vectors[doc_id][word_id] = tf
                word_counter[word_id] += 1
        
        # Второй проход: вычисление TF-IDF и сохранение
        total_docs = len(documents)
        for doc_id, word_tf in document_vectors.items():
            for word_id, tf in word_tf.items():
                df = word_counter[word_id]
                idf = math.log((total_docs + 1) / (df + 1)) + 1  # smoothing
                tf_idf = tf * idf
                
                cursor.execute("""
                    INSERT INTO document_words (doc_id, word_id, tf)
                    VALUES (?, ?, ?)
                    ON CONFLICT(doc_id, word_id) DO UPDATE SET tf = ?
                """, (doc_id, word_id, tf_idf, tf_idf))
        
        # Обновляем статистику слов
        for word_id, df in word_counter.items():
            cursor.execute("""
                INSERT OR REPLACE INTO word_stats (word_id, df)
                VALUES (?, ?)
            """, (word_id, df))
        
        # Помечаем документы как проиндексированные
        cursor.execute("UPDATE documents SET indexed = TRUE WHERE indexed = FALSE")
        
        self.conn.commit()
        print(f"Indexed {len(documents)} documents with {len(word_to_id)} unique words.")
    
    def get_document_count(self) -> int:
        """Получение общего количества документов"""
        cursor = self.conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM documents")
        return cursor.fetchone()[0]
    
    def get_all_links(self) -> List[Tuple[int, int]]:
        """Получение всех ссылок"""
        cursor = self.conn.cursor()
        cursor.execute("SELECT source_id, target_id FROM links")
        return cursor.fetchall()
    
    def get_outgoing_links(self, doc_id: int) -> List[int]:
        """Получение исходящих ссылок"""
        cursor = self.conn.cursor()
        cursor.execute("SELECT target_id FROM links WHERE source_id = ?", (doc_id,))
        return [row[0] for row in cursor.fetchall()]
    
    def get_incoming_links(self, doc_id: int) -> List[int]:
        """Получение входящих ссылок"""
        cursor = self.conn.cursor()
        cursor.execute("SELECT source_id FROM links WHERE target_id = ?", (doc_id,))
        return [row[0] for row in cursor.fetchall()]
    
    def update_pagerank(self, pageranks: Dict[int, float]):
        """Обновление PageRank в базе данных"""
        cursor = self.conn.cursor()
        for doc_id, rank in pageranks.items():
            cursor.execute(
                "UPDATE documents SET pagerank = ? WHERE id = ?",
                (rank, doc_id)
            )
        self.conn.commit()
    
    def search_by_words(self, word_ids: List[int]) -> List[int]:
        """Поиск документов по ID слов"""
        cursor = self.conn.cursor()
        placeholders = ','.join('?' for _ in word_ids)
        query = f"""
            SELECT doc_id, SUM(tf) as score
            FROM document_words
            WHERE word_id IN ({placeholders})
            GROUP BY doc_id
            ORDER BY score DESC
            LIMIT 100
        """
        cursor.execute(query, word_ids)
        return [(row['doc_id'], row['score']) for row in cursor.fetchall()]
    
    def get_document_info(self, doc_id: int) -> Dict:
        """Получение информации о документе"""
        cursor = self.conn.cursor()
        cursor.execute(
            "SELECT id, url, title, pagerank FROM documents WHERE id = ?",
            (doc_id,)
        )
        row = cursor.fetchone()
        return dict(row) if row else None

    def has_data(self) -> bool:
        """Проверяет, есть ли уже данные в базе"""
        cursor = self.conn.cursor()
        
        # Проверяем несколько критериев
        cursor.execute("SELECT COUNT(*) FROM documents")
        doc_count = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM links")
        link_count = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM document_words")
        index_count = cursor.fetchone()[0]
        
        # Считаем, что данные есть, если документов больше 5
        # и есть связи или индекс
        return doc_count > 5 and (link_count > 0 or index_count > 0)

    def get_stats(self) -> Dict[str, int]:
        """Возвращает статистику базы данных"""
        cursor = self.conn.cursor()
        
        stats = {}
        cursor.execute("SELECT COUNT(*) FROM documents")
        stats['documents'] = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM words")
        stats['words'] = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM links")
        stats['links'] = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM document_words")
        stats['indexed_terms'] = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(DISTINCT doc_id) FROM document_words")
        stats['indexed_documents'] = cursor.fetchone()[0]
        
        return stats
