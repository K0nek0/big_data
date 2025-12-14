import sqlite3
from typing import Dict, List
from pregel import Vertex, Pregel


class PageRankVertex(Vertex):
    """Вершина для вычисления PageRank с использованием Pregel"""
    
    def update(self):
        """Метод update вызывается на каждом суперстепе"""
        if self.superstep < 30:  # Ограничиваем количество суперстепов
            # Обрабатываем входящие сообщения (вклад от других вершин)
            total = 0.0
            for (sender, message) in self.incoming_messages:
                total += message
            
            # Вычисляем новый PageRank
            damping_factor = 0.85
            new_value = (1 - damping_factor) + damping_factor * total
            
            # Проверяем сходимость
            if abs(new_value - self.value) < 0.0001:
                self.active = False
            else:
                self.active = True
            
            self.value = new_value
            
            # Отправляем сообщения на следующем суперстепе
            self.outgoing_messages = []
            if self.out_vertices:
                share = self.value / len(self.out_vertices)
                for vertex in self.out_vertices:
                    self.outgoing_messages.append((vertex, share))
            else:
                # Если нет исходящих ссылок, отправляем всем
                for vertex in self.out_vertices:
                    self.outgoing_messages.append((vertex, self.value / len(self.out_vertices)))
        else:
            self.active = False


class PageRankPregel:
    """Класс для вычисления PageRank с использованием Pregel"""
    
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.vertices = []
        self.vertex_map = {}  # Map from doc_id to Vertex object
        
    def build_graph_from_db(self):
        """Построение графа из базы данных"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Получаем все документы
        cursor.execute("SELECT id FROM documents")
        doc_ids = [row[0] for row in cursor.fetchall()]
        
        # Создаем вершины с начальным значением 1.0
        for doc_id in doc_ids:
            vertex = PageRankVertex(id=doc_id, value=1.0, out_vertices=[])
            self.vertices.append(vertex)
            self.vertex_map[doc_id] = vertex
        
        # Получаем все ссылки и устанавливаем связи между вершинами
        cursor.execute("SELECT source_id, target_id FROM links")
        links = cursor.fetchall()
        
        link_count = 0
        for source_id, target_id in links:
            if source_id in self.vertex_map and target_id in self.vertex_map:
                source_vertex = self.vertex_map[source_id]
                target_vertex = self.vertex_map[target_id]
                source_vertex.out_vertices.append(target_vertex)
                link_count += 1
        
        conn.close()
        
        # Инициализируем значения PageRank
        initial_value = 1.0 / len(self.vertices) if self.vertices else 0
        for vertex in self.vertices:
            vertex.value = initial_value
        
        print(f"Граф построен: {len(self.vertices)} вершин, {link_count} связей")
        return self.vertices
    
    def run_pregel(self, num_workers: int = 4):
        """Запуск Pregel для вычисления PageRank"""
        if not self.vertices:
            self.build_graph_from_db()
        
        print(f"Запуск Pregel с {num_workers} воркерами...")
        
        # Создаем и запускаем Pregel
        pregel = Pregel(self.vertices, num_workers)
        pregel.run()
        
        # Собираем результаты
        results = {}
        total_rank = sum(v.value for v in self.vertices)
        
        # Нормализуем значения
        for vertex in self.vertices:
            normalized_value = vertex.value / total_rank if total_rank > 0 else 0
            results[vertex.id] = normalized_value
        
        print(f"PageRank вычислен для {len(results)} документов")
        return results
    
    def update_database(self):
        """Обновление значений PageRank в базе данных"""
        pageranks = self.run_pregel()
        
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        updated_count = 0
        for doc_id, rank in pageranks.items():
            cursor.execute(
                "UPDATE documents SET pagerank = ? WHERE id = ?",
                (rank, doc_id)
            )
            updated_count += cursor.rowcount
        
        conn.commit()
        conn.close()
        
        print(f"Обновлено {updated_count} записей в базе данных")
        return pageranks
    
    def get_top_documents(self, limit: int = 10) -> List[Dict]:
        """Получение топ-N документов по PageRank"""
        conn = sqlite3.connect(self.db_path)
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
