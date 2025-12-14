import requests
from bs4 import BeautifulSoup
import urllib.parse
import re
import time
from typing import List, Dict, Set, Tuple
import sqlite3
from concurrent.futures import ThreadPoolExecutor, as_completed
from config import Config
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class WebCrawler:
    def __init__(self):
        self.visited_urls: Set[str] = set()
        self.to_visit: List[Tuple[str, int]] = [(url, 0) for url in Config.START_URLS]
        self.url_to_id: Dict[str, int] = {}
        self.session = requests.Session()
        self.session.headers.update({'User-Agent': Config.USER_AGENT})
        
    def normalize_url(self, url: str, base_url: str) -> str:
        """Нормализация URL"""
        try:
            parsed = urllib.parse.urlparse(url)
            if not parsed.scheme:
                url = urllib.parse.urljoin(base_url, url)
            
            parsed = urllib.parse.urlparse(url)
            # Убираем фрагменты и параметры запросов
            normalized = urllib.parse.urlunparse(
                (parsed.scheme, parsed.netloc, parsed.path, '', '', '')
            )
            return normalized.rstrip('/')
        except:
            return None
    
    def extract_text(self, soup: BeautifulSoup) -> str:
        """Извлечение текста из HTML"""
        # Удаляем скрипты и стили
        for script in soup(["script", "style", "nav", "footer", "header"]):
            script.decompose()
        
        text = soup.get_text(separator=' ', strip=True)
        # Убираем лишние пробелы
        text = re.sub(r'\s+', ' ', text)
        return text
    
    def extract_links(self, soup: BeautifulSoup, base_url: str) -> List[str]:
        """Извлечение ссылок из HTML"""
        links = set()
        for link in soup.find_all('a', href=True):
            href = link['href']
            normalized = self.normalize_url(href, base_url)
            if normalized and normalized.startswith('http'):
                links.add(normalized)
        return list(links)
    
    def process_page(self, url: str) -> Tuple[str, str, str, List[str]]:
        """Обработка одной страницы"""
        try:
            response = self.session.get(url, timeout=10)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'lxml')
            title = soup.title.string if soup.title else url
            
            # Извлекаем основной контент (стараемся найти article или main)
            main_content = soup.find('article') or soup.find('main') or soup.body
            if main_content:
                content = self.extract_text(main_content)
            else:
                content = self.extract_text(soup)
            
            links = self.extract_links(soup, url)
            
            return url, title, content, links
            
        except Exception as e:
            logger.error(f"Error processing {url}: {e}")
            return None
    
    def save_to_database(self, url: str, title: str, content: str, links: List[str]):
        """Сохранение данных в базу"""
        conn = sqlite3.connect(Config.DB_PATH)
        cursor = conn.cursor()
        
        try:
            # Сохраняем документ
            cursor.execute(
                "INSERT OR IGNORE INTO documents (url, title, content) VALUES (?, ?, ?)",
                (url, title, content)
            )
            
            # Получаем ID документа
            cursor.execute("SELECT id FROM documents WHERE url = ?", (url,))
            doc_id = cursor.fetchone()[0]
            self.url_to_id[url] = doc_id
            
            # Сохраняем ссылки
            for link in links:
                if link in self.url_to_id:
                    target_id = self.url_to_id[link]
                    cursor.execute(
                        "INSERT OR IGNORE INTO links (source_id, target_id) VALUES (?, ?)",
                        (doc_id, target_id)
                    )
            
            conn.commit()
            
        except Exception as e:
            logger.error(f"Error saving to database: {e}")
            conn.rollback()
        finally:
            conn.close()
    
    def crawl(self):
        """Основной метод краулинга"""
        logger.info("Starting web crawler...")
        
        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = {}
            
            while self.to_visit and len(self.visited_urls) < Config.MAX_PAGES:
                url, depth = self.to_visit.pop(0)
                
                if url in self.visited_urls or depth > Config.MAX_DEPTH:
                    continue
                
                self.visited_urls.add(url)
                logger.info(f"Crawling: {url} (depth: {depth})")
                
                # Отправляем задачу на обработку
                future = executor.submit(self.process_page, url)
                futures[future] = (url, depth)
                
                # Обрабатываем завершенные задачи
                for future in as_completed(futures):
                    url, depth = futures.pop(future)
                    
                    try:
                        result = future.result()
                        if result:
                            page_url, title, content, links = result
                            
                            # Сохраняем в базу
                            self.save_to_database(page_url, title, content, links)
                            
                            # Добавляем новые ссылки в очередь
                            for link in links:
                                if (link not in self.visited_urls and 
                                    link not in [u for u, _ in self.to_visit]):
                                    self.to_visit.append((link, depth + 1))
                    
                    except Exception as e:
                        logger.error(f"Error in future: {e}")
                
                time.sleep(0.5)  # Вежливая задержка
        
        logger.info(f"Crawling completed. Visited {len(self.visited_urls)} pages.")
