import sqlite3
import re
import math
from typing import List, Dict, Tuple
from collections import defaultdict
from config import Config


class SearchEngine:
    def __init__(self):
        self.conn = sqlite3.connect(Config.DB_PATH)
        self.conn.row_factory = sqlite3.Row
    
    def tokenize_query(self, query: str) -> List[str]:
        """Токенизация поискового запроса"""
        words = re.findall(r'\b[a-z]{2,}\b', query.lower())
        return [w for w in words if w not in Config.STOP_WORDS]
    
    def get_word_ids(self, words: List[str]) -> List[int]:
        """Получение ID слов из базы данных"""
        cursor = self.conn.cursor()
        word_ids = []
        
        for word in words:
            cursor.execute("SELECT id FROM words WHERE word = ?", (word,))
            result = cursor.fetchone()
            if result:
                word_ids.append(result[0])
        
        return word_ids
    
    def document_at_a_time(self, word_ids: List[int], k: int = 10) -> List[Tuple[int, float]]:
        """
        Document-at-a-time поиск
        Обрабатываем все термины для каждого документа
        """
        if not word_ids:
            return []
        
        cursor = self.conn.cursor()
        
        # Получаем документы, содержащие хотя бы одно слово
        placeholders = ','.join('?' for _ in word_ids)
        query = f"""
            SELECT DISTINCT doc_id 
            FROM document_words 
            WHERE word_id IN ({placeholders})
        """
        cursor.execute(query, word_ids)
        doc_ids = [row[0] for row in cursor.fetchall()]
        
        results = []
        
        # Для каждого документа вычисляем релевантность
        for doc_id in doc_ids:
            score = 0.0
            
            # Получаем TF-IDF для всех слов запроса
            for word_id in word_ids:
                cursor.execute("""
                    SELECT tf FROM document_words 
                    WHERE doc_id = ? AND word_id = ?
                """, (doc_id, word_id))
                
                result = cursor.fetchone()
                if result:
                    tf_idf = result[0]
                    
                    # Получаем IDF
                    cursor.execute("SELECT df FROM word_stats WHERE word_id = ?", (word_id,))
                    df_result = cursor.fetchone()
                    
                    if df_result:
                        total_docs = self.get_document_count()
                        df = df_result[0]
                        idf = math.log((total_docs + 1) / (df + 1)) + 1
                        
                        # Взвешенный вклад слова
                        score += tf_idf * idf
            
            # Учитываем PageRank
            cursor.execute("SELECT pagerank FROM documents WHERE id = ?", (doc_id,))
            pagerank_result = cursor.fetchone()
            if pagerank_result:
                pagerank = pagerank_result[0]
                final_score = 0.7 * score + 0.3 * pagerank * 100  # Масштабируем PageRank
                results.append((doc_id, final_score))
        
        # Сортировка по убыванию релевантности
        results.sort(key=lambda x: x[1], reverse=True)
        return results[:k]
    
    def term_at_a_time(self, word_ids: List[int], k: int = 10) -> List[Tuple[int, float]]:
        """
        Term-at-a-time поиск
        Обрабатываем все документы для каждого термина
        """
        if not word_ids:
            return []

        cursor = self.conn.cursor()
        accumulators = defaultdict(float)

        total_docs = self.get_document_count()

        # Для каждого слова в запросе
        for word_id in word_ids:
            # Получаем все документы, содержащие это слово
            cursor.execute("""
                SELECT doc_id, tf FROM document_words
                WHERE word_id = ?
            """, (word_id,))
            
            docs_for_word = cursor.fetchall()  # Сохраняем результаты первого запроса

            # Получаем IDF для слова
            cursor.execute("SELECT df FROM word_stats WHERE word_id = ?", (word_id,))
            df_result = cursor.fetchone()

            if df_result:
                df = df_result[0]
                idf = math.log((total_docs + 1) / (df + 1)) + 1

                for row in docs_for_word:  # Используем сохраненные результаты
                    doc_id = row[0]
                    tf_idf = row[1]
                    accumulators[doc_id] += tf_idf * idf

        # Преобразуем в список и добавляем PageRank
        results = []
        for doc_id, score in accumulators.items():
            cursor.execute("SELECT pagerank FROM documents WHERE id = ?", (doc_id,))
            pagerank_result = cursor.fetchone()

            if pagerank_result:
                pagerank = pagerank_result[0]
                final_score = 0.7 * score + 0.3 * pagerank * 100
                results.append((doc_id, final_score))
        
        # Сортировка по убыванию релевантности
        results.sort(key=lambda x: x[1], reverse=True)
        return results[:k]
    
    def get_document_count(self) -> int:
        """Получение общего количества документов"""
        cursor = self.conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM documents")
        return cursor.fetchone()[0]
    
    def search(self, query: str, method: str = 'doc_at_a_time', k: int = 10) -> List[Dict]:
        """Основной метод поиска"""
        # Токенизация запроса
        words = self.tokenize_query(query)
        
        if not words:
            return []
        
        # Получение ID слов
        word_ids = self.get_word_ids(words)
        
        if not word_ids:
            return []
        
        # Выбор метода поиска
        if method == 'doc_at_a_time':
            results = self.document_at_a_time(word_ids, k)
        elif method == 'term_at_a_time':
            results = self.term_at_a_time(word_ids, k)
        else:
            raise ValueError("Invalid search method")
        
        # Получение полной информации о документах
        cursor = self.conn.cursor()
        final_results = []
        
        for doc_id, score in results:
            cursor.execute("""
                SELECT id, url, title, pagerank 
                FROM documents 
                WHERE id = ?
            """, (doc_id,))
            
            row = cursor.fetchone()
            if row:
                result_dict = dict(row)
                result_dict['score'] = score
                result_dict['matched_words'] = words
                final_results.append(result_dict)
        
        return final_results

    def close(self):
        self.conn.close()
