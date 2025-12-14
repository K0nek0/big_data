import time
import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from crawler import WebCrawler
from database import DatabaseManager
from pagerank_mapreduce import MapReducePageRank
from pagerank_pregel import PageRankPregel
from search_engine import SearchEngine
from config import Config


def print_header(text: str):
    """Печать заголовка"""
    print("\n" + "=" * 80)
    print(f" {text.upper()}")
    print("=" * 80)


def demo_full_pipeline():
    """Полная демонстрация всех компонентов системы"""
    
    # 1. Инициализация базы данных
    print_header("1. Инициализация системы")
    Config.init_database()
    
    db = DatabaseManager()
    
    # Объявляем переменные заранее
    mr_time = 0
    pregel_time = 0
    pagerank_mr = None
    pagerank_pregel = None
    
    # Проверяем, есть ли уже документы
    doc_count = db.get_document_count()
    if doc_count > 10:
        print(f"2. В базе уже есть {doc_count} документов. Пропускаем краулинг.")
    else:
        # 2. Сбор данных
        print_header("2. Сбор данных с веб-сайтов")
        crawler = WebCrawler()
        crawler.crawl()
    
    # Проверяем, построен ли индекс
    cursor = db.conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM document_words")
    index_count = cursor.fetchone()[0]
    
    if index_count > 0:
        print(f"3. Индекс уже построен ({index_count} записей). Пропускаем индексирование.")
    else:
        # 3. Индексирование
        print_header("3. Индексирование документов")
        db.build_index()
    
    # Проверяем, вычислен ли PageRank (не равен значению по умолчанию 1.0)
    cursor.execute("SELECT COUNT(*) FROM documents WHERE pagerank != 1.0")
    pr_calculated = cursor.fetchone()[0]
    
    if pr_calculated > 0:
        print(f"4-5. PageRank уже вычислен для {pr_calculated} документов.")
        print("\nИспользуем существующие значения из базы.")
        
        # Создаем объекты для доступа к методам get_top_documents
        pagerank_mr = MapReducePageRank()
        pagerank_pregel = PageRankPregel(Config.DB_PATH)
        
    else:
        # 4. PageRank через MapReduce
        print_header("4. PageRank через MapReduce")
        start_time = time.time()
        pagerank_mr = MapReducePageRank()
        pagerank_mr.update_database()
        mr_time = time.time() - start_time
        
        # 5. PageRank через Pregel
        print_header("5. PageRank через Pregel")
        start_time = time.time()
        pagerank_pregel = PageRankPregel(Config.DB_PATH)
        pagerank_pregel.update_database()
        pregel_time = time.time() - start_time
        
        print(f"\nВремя выполнения MapReduce: {mr_time:.2f} сек")
        print(f"Время выполнения Pregel: {pregel_time:.2f} сек")
    
    # 6. Топ документы по PageRank
    print_header("6. Топ-10 документов по PageRank")
    
    if pagerank_mr and pagerank_pregel:
        print("\nMapReduce результаты:")
        top_mr = pagerank_mr.get_top_documents(5)
        for i, doc in enumerate(top_mr, 1):
            print(f"  {i}. {doc['title'][:50]}... - PR: {doc['pagerank']:.6f}")
        
        print("\nPregel результаты:")
        top_pregel = pagerank_pregel.get_top_documents(5)
        for i, doc in enumerate(top_pregel, 1):
            print(f"  {i}. {doc['title'][:50]}... - PR: {doc['pagerank']:.6f}")
    
    # 7. Поиск
    print_header("7. Демонстрация поиска")
    
    se = SearchEngine()
    
    test_queries = [
        "machine learning algorithm",
        "data science techniques",
        "web crawler search",
        "artificial intelligence future"
    ]
    
    for query in test_queries:
        print(f"\n🔍 Запрос: '{query}'")
        print("-" * 70)
        
        # Document-at-a-time
        print("Document-at-a-time подход:")
        results_doc = se.search(query, method='doc_at_a_time', k=3)
        for i, result in enumerate(results_doc, 1):
            print(f"  {i}. {result['title']}")
            print(f"     Рейтинг: {result['score']:.2f} | PR: {result['pagerank']:.6f}")
        
        # Term-at-a-time
        print("\nTerm-at-a-time подход:")
        results_term = se.search(query, method='term_at_a_time', k=3)
        for i, result in enumerate(results_term, 1):
            print(f"  {i}. {result['title']}")
            print(f"     Рейтинг: {result['score']:.2f} | PR: {result['pagerank']:.6f}")
    
    # 8. Интерактивный поиск
    print_header("8. Интерактивный поиск")
    
    while True:
        print("\nВведите поисковый запрос (или 'quit' для выхода):")
        query = input("> ").strip()
        
        if query.lower() in ['quit', 'exit', 'q']:
            break
        
        if not query:
            continue
        
        print(f"\nРезультаты поиска для: '{query}'")
        print("-" * 70)
        
        # Document-at-a-time
        results_doc = se.search(query, method='doc_at_a_time', k=5)
        print("\n📄 Document-at-a-time (первые 5):")
        for i, result in enumerate(results_doc, 1):
            print(f"  {i}. [{result['score']:.2f}] {result['title']}")
            print(f"     {result['url']}")
        
        # Term-at-a-time
        results_term = se.search(query, method='term_at_a_time', k=5)
        print("\n🔤 Term-at-a-time (первые 5):")
        for i, result in enumerate(results_term, 1):
            print(f"  {i}. [{result['score']:.2f}] {result['title']}")
            print(f"     {result['url']}")
    
    se.close()
    db.close()
    
    print_header("Демонстрация завершена!")


if __name__ == "__main__":
    demo_full_pipeline()
