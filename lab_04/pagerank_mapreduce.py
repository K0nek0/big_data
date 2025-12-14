import sqlite3
from typing import Dict, List, Tuple
from collections import defaultdict
from config import Config
from database import DatabaseManager


class MapReducePageRank:
    def __init__(self):
        self.db = DatabaseManager()
        self.N = self.db.get_document_count()
        self.damping = Config.DAMPING_FACTOR
    
    def mapper(self, doc_id: int) -> List[Tuple[int, float]]:
        """Map функция: распределение PageRank по исходящим ссылкам"""
        outgoing_links = self.db.get_outgoing_links(doc_id)
        
        if not outgoing_links:
            # Если нет исходящих ссылок, распределяем по всем документам
            return [(i, self.current_ranks[doc_id] / self.N) for i in range(1, self.N + 1)]
        
        # Распределяем равномерно по исходящим ссылкам
        share = self.current_ranks[doc_id] / len(outgoing_links)
        return [(target_id, share) for target_id in outgoing_links]
    
    def reducer(self, doc_id: int, contributions: List[float]) -> float:
        """Reduce функция: суммирование вкладов и применение формулы PageRank"""
        total_contribution = sum(contributions)
        
        # Формула PageRank
        new_rank = (1 - self.damping) / self.N + self.damping * total_contribution
        return new_rank
    
    def run_mapreduce(self, iterations: int = Config.MAX_ITERATIONS) -> Dict[int, float]:
        """Запуск MapReduce для вычисления PageRank"""
        print("Starting MapReduce PageRank computation...")
        
        # Инициализация рангов
        self.current_ranks = {doc_id: 1.0 / self.N for doc_id in range(1, self.N + 1)}
        
        for iteration in range(iterations):
            print(f"Iteration {iteration + 1}/{iterations}")
            
            # Map phase
            contributions = defaultdict(list)
            for doc_id in range(1, self.N + 1):
                mapped_pairs = self.mapper(doc_id)
                for target_id, contribution in mapped_pairs:
                    contributions[target_id].append(contribution)
            
            # Reduce phase
            new_ranks = {}
            for doc_id in range(1, self.N + 1):
                if doc_id in contributions:
                    new_rank = self.reducer(doc_id, contributions[doc_id])
                else:
                    # Если нет входящих ссылок
                    new_rank = (1 - self.damping) / self.N
                new_ranks[doc_id] = new_rank
            
            # Проверка сходимости
            total_change = sum(abs(new_ranks[doc_id] - self.current_ranks[doc_id]) 
                             for doc_id in range(1, self.N + 1))
            
            print(f"  Total change: {total_change:.6f}")
            
            if total_change < Config.TOLERANCE:
                print(f"Converged after {iteration + 1} iterations")
                break
            
            self.current_ranks = new_ranks
        
        # Нормализация
        total = sum(self.current_ranks.values())
        self.current_ranks = {k: v / total for k, v in self.current_ranks.items()}
        
        return self.current_ranks
    
    def update_database(self):
        """Обновление PageRank в базе данных"""
        pageranks = self.run_mapreduce()
        self.db.update_pagerank(pageranks)
        print("PageRank values updated in database.")
    
    def get_top_documents(self, limit: int = 10) -> List[Dict]:
        """Получение топ-N документов по PageRank из базы данных"""
        conn = sqlite3.connect(Config.DB_PATH)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT id, url, title, pagerank 
            FROM documents 
            ORDER BY pagerank DESC 
            LIMIT ?
        """, (limit,))
        
        results = []
        for row in cursor.fetchall():
            results.append(dict(row))
        
        conn.close()
        return results
