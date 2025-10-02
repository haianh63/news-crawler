from cleaner import Cleaner
import requests
def get_html(url):
        """Tải HTML từ URL và làm sạch nó."""
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        response = requests.get(url, headers=headers, timeout=10)
        if response.status_code == 200:
            with open("output.html", "w", encoding="utf-8") as f:
                f.write(response.text)
            return response.text
        else:
            raise Exception(f"Failed to fetch {url}: {response.status_code}")
        

url = 'https://laodong.vn/the-gioi/bao-kiko-vua-hinh-thanh-du-bao-manh-len-thanh-bao-cuong-phong-vao-29-1567211.ldo'
cleaner = Cleaner()
cleaned = cleaner.clean_html(get_html(url))

with open("cleaned.html", "w", encoding="utf-8") as f:
    f.write(cleaned)