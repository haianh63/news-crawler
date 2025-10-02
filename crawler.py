import requests
from bs4 import BeautifulSoup
from pymongo import MongoClient
import google.generativeai as genai
from datetime import datetime
from urllib.parse import urlparse
from cleaner import Cleaner

class NewsCrawler:
    def __init__(self):
        GEMINI_API_KEY = "AIzaSyA0ugl8fky3FvhUsKtFRflGtEDV2nKUTIw"
        MONGO_URI = "mongodb://localhost:27017/"
        genai.configure(api_key=GEMINI_API_KEY)
        self.model = genai.GenerativeModel('gemini-1.5-flash') 
        client = MongoClient(MONGO_URI)
        db = client["crawler_db"]
        self.rules_collection = db["domain_rules"]

        self.cleaner = Cleaner()

    def clean_response(self, response):
        """Làm sạch phản hồi từ Gemini."""
        cleaned = response.replace("```json", "").replace("```", "").strip()
        return cleaned

    def get_html(self, url):
        """Tải HTML từ URL và làm sạch nó."""
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        response = requests.get(url, headers=headers, timeout=10)
        if response.status_code == 200:
            return self.cleaner.clean_html(response.text)
        else:
            raise Exception(f"Failed to fetch {url}: {response.status_code}")
    
    def get_domain(self, url):
        parsed = urlparse(url)
        return parsed.netloc

    def get_rules_from_db(self, domain):
        docs = self.rules_collection.find({"domain": domain})
        rules = [doc["rules"] for doc in docs if "rules" in doc]
        return rules if rules else None
        

    def save_rules_to_db(self, domain, rules):
        self.rules_collection.insert_one({
            "domain": domain,
            "rules": rules,
            "last_updated": datetime.now().isoformat()
        })

    def generate_rules_with_gemini(self, html):
        prompt = f"""
        Từ HTML của bài báo này, hãy tìm ra các CSS selector để extract:
        - Tiêu đề bài báo (thường là h1 hoặc h2 chính).
        - Nội dung chính (thường là div chứa đoạn văn, loại bỏ header/footer/ads).
        - Tác giả bài báo (thường là tên tác giả trong thẻ span, div, hoặc p gần tiêu đề hoặc cuối bài).
        Trả về định dạng JSON chính xác sau (không thêm text thừa):
        {{
            "title_selector": "CSS selector cho title",
            "content_selector": "CSS selector cho content (lấy text từ tất cả p bên trong)",
            "author_selector": "CSS selector cho tên tác giả"
        }}
        HTML: {html[:10000]}
        """
        
        try:
            response = self.model.generate_content(prompt)
            cleaned_response = self.clean_response(response.text)
            print(cleaned_response)
            import json
            rules = json.loads(cleaned_response)
            return rules
        except Exception as e:
            raise Exception(f"Failed to parse Gemini response: {e}")
        
    def extract_data_with_bs(self, html, rules):
        soup = BeautifulSoup(html, 'html.parser')
        title_elem = soup.select_one(rules["title_selector"])
        title = title_elem.text.strip() if title_elem else "Not found"
        
        content_elems = soup.select(rules["content_selector"])
        if content_elems:
            content = "\n".join(elem.get_text(strip=True) for elem in content_elems)
        else:
            content = "Not found"
        author_elem = soup.select_one(rules["author_selector"])
        author = author_elem.text.strip() if author_elem else "Not found"
        return {
            "title": title,
            "content": content,
            "author": author
        }

    def crawl_article(self, url):
        try:
            html = self.get_html(url)
            with open("output.html", "w", encoding="utf-8") as f:
                f.write(html)
            domain = self.get_domain(url)
            rules = self.get_rules_from_db(domain)
            
            if not rules:
                print(f"No rules for {domain}, generating with Gemini...")
                rule = self.generate_rules_with_gemini(html)
                self.save_rules_to_db(domain, rule)
                rules = [rule]
            
            for rule in rules:
                data = self.extract_data_with_bs(html, rule)
                if data['author'] != 'Not found':
                    return data
                
            print(f"No rules for {domain}, generating with Gemini...")
            rule = self.generate_rules_with_gemini(html)
            self.save_rules_to_db(domain, rule)
            data = self.extract_data_with_bs(html, rule)
            return data
            
                
        except Exception as e:
            return {"error": str(e)}
