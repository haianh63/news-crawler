import feedparser
from pymongo import MongoClient
from kafka import KafkaProducer
from datetime import datetime
import json
import logging
import schedule
import time
from datetime import datetime
from bs4 import BeautifulSoup
from crawler import NewsCrawler
from checker import TitleDuplicateChecker
# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

client = MongoClient("mongodb://localhost:27017/")
db = client["crawler_db"]
articles_collection = db["articles"]
crawler = NewsCrawler()

producer = KafkaProducer(bootstrap_servers='localhost:9092',
                        value_serializer=lambda v: json.dumps(v).encode('utf-8'))

TOPICS = [
    'Xã hội', 'Thế giới', 'Kinh tế', 'Đời sống', 'Sức khoẻ',
    'Giáo dục', 'Thể thao', 'Giải trí', 'Du lịch',
    'Pháp luật', 'Khoa học - Công nghệ', 'Xe'
]

CHECKERS = {
    topic: TitleDuplicateChecker(similarity_threshold=0.70)
    for topic in TOPICS
}
RSS_FEEDS = [
    [
        # ('Thế giới', "https://vnexpress.net/rss/the-gioi.rss", "VnExpress"),
        # ('Xã hội', "https://vnexpress.net/rss/thoi-su.rss", "VnExpress"),
        # ('Kinh tế', "https://vnexpress.net/rss/kinh-doanh.rss", "VnExpress"),
        # ('Kinh tế', "https://vnexpress.net/rss/startup.rss", "VnExpress"),
        # ('Giải trí', "https://vnexpress.net/rss/giai-tri.rss", "VnExpress"),
        # ('Thể thao', "https://vnexpress.net/rss/the-thao.rss", "VnExpress"),
        # ('Pháp luật', "https://vnexpress.net/rss/phap-luat.rss", "VnExpress"),
        # ('Giáo dục', "https://vnexpress.net/rss/giao-duc.rss", "VnExpress"),
        # ('Sức khoẻ', "https://vnexpress.net/rss/suc-khoe.rss", "VnExpress"),
        # ('Đời sống', "https://vnexpress.net/rss/gia-dinh.rss", "VnExpress"),
        ('Du lịch', "https://vnexpress.net/rss/du-lich.rss", "VnExpress"),
        ('Khoa học - Công nghệ', "https://vnexpress.net/rss/khoa-hoc-cong-nghe.rss", "VnExpress"),
        ('Xe', "https://vnexpress.net/rss/oto-xe-may.rss", "VnExpress"),
    ],
    [
        # ('Xã hội', 'https://thanhnien.vn/rss/thoi-su.rss', "Thanh niên"),
        # ('Xã hội', 'https://thanhnien.vn/rss/chinh-tri.rss', "Thanh niên"),
        # ('Thế giới', 'https://thanhnien.vn/rss/the-gioi.rss', "Thanh niên"),
        # ('Kinh tế', 'https://thanhnien.vn/rss/kinh-te.rss', "Thanh niên"),
        # ('Đời sống', 'https://thanhnien.vn/rss/doi-song.rss', "Thanh niên"),
        # ('Sức khoẻ', 'https://thanhnien.vn/rss/suc-khoe.rss', "Thanh niên"),
        # ('Giáo dục', 'https://thanhnien.vn/rss/giao-duc.rss', "Thanh niên"),
        # ('Du lịch', 'https://thanhnien.vn/rss/du-lich.rss', "Thanh niên"),
        # ('Giải trí', 'https://thanhnien.vn/rss/van-hoa.rss', "Thanh niên"),
        # ('Giải trí', 'https://thanhnien.vn/rss/giai-tri.rss', "Thanh niên"),
        # ('Thể thao', 'https://thanhnien.vn/rss/the-thao.rss', "Thanh niên"),
        # ('Khoa học - Công nghệ', 'https://thanhnien.vn/rss/cong-nghe.rss', "Thanh niên"),
        # ('Xe', 'https://thanhnien.vn/rss/xe.rss', "Thanh niên"),
    ],

    [
        # ('Xã hội', 'https://dantri.com.vn/rss/xa-hoi.rss', "Dân trí"),
        # ('Thế giới', 'https://dantri.com.vn/rss/the-gioi.rss', "Dân trí"),
        # ('Kinh tế', 'https://dantri.com.vn/rss/kinh-doanh.rss', "Dân trí"),
        # ('Kinh tế', 'https://dantri.com.vn/rss/bat-dong-san.rss', "Dân trí"),
        # ('Đời sống', 'https://dantri.com.vn/rss/doi-song.rss', "Dân trí"),
        # ('Sức khoẻ', 'https://dantri.com.vn/rss/suc-khoe.rss', "Dân trí"),
        # ('Giáo dục', 'https://dantri.com.vn/rss/giao-duc.rss', "Dân trí"),
        # ('Thể thao', 'https://dantri.com.vn/rss/the-thao.rss', "Dân trí"),
        # ('Giải trí', 'https://dantri.com.vn/rss/giai-tri.rss', "Dân trí"),
        # ('Du lịch', 'https://dantri.com.vn/rss/du-lich.rss', "Dân trí"),
        # ('Pháp luật', 'https://dantri.com.vn/rss/phap-luat.rss', "Dân trí"),
        # ('Khoa học - Công nghệ', 'https://dantri.com.vn/rss/cong-nghe.rss', "Dân trí"),
        # ('Khoa học - Công nghệ', 'https://dantri.com.vn/rss/khoa-hoc.rss', "Dân trí"),
        # ('Xe', 'https://dantri.com.vn/rss/o-to-xe-may.rss', "Dân trí"),
    ],

    [
        # ('Xã hội', 'https://nhandan.vn/rss/xahoi-1211.rss', "Nhân dân"),
        # ('Xã hội', 'https://nhandan.vn/rss/chinhtri-1171.rss', "Nhân dân"),
        # ('Xã hội', 'https://nhandan.vn/rss/xa-luan-1176.rss', "Nhân dân"),
        # ('Xã hội', 'https://nhandan.vn/rss/binh-luan-phe-phan-1180.rss', "Nhân dân"),
        # ('Xã hội', 'https://nhandan.vn/rss/xay-dung-dang-1179.rss', "Nhân dân"),
        # ('Thế giới', 'https://nhandan.vn/rss/thegioi-1231.rss', "Nhân dân"),
        # ('Thế giới', 'https://nhandan.vn/rss/binh-luan-quoc-te-1236.rss', "Nhân dân"),
        # ('Thế giới', 'https://nhandan.vn/rss/asean-704471.rss', "Nhân dân"),
        # ('Thế giới', 'https://nhandan.vn/rss/chau-phi-704476.rss', "Nhân dân"),
        # ('Thế giới', 'https://nhandan.vn/rss/chau-my-704475.rss', "Nhân dân"),
        # ('Thế giới', 'https://nhandan.vn/rss/chau-au-704474.rss', "Nhân dân"),
        # ('Thế giới', 'https://nhandan.vn/rss/trung-dong-704473.rss', "Nhân dân"),
        # ('Thế giới', 'https://nhandan.vn/rss/chau-a-tbd-704472.rss', "Nhân dân"),
        # ('Kinh tế', 'https://nhandan.vn/rss/kinhte-1185.rss', "Nhân dân"),
        # ('Kinh tế', 'https://nhandan.vn/rss/chungkhoan-1191.rss', "Nhân dân"),
        # ('Kinh tế', 'https://nhandan.vn/rss/thong-tin-hang-hoa-1203.rss', "Nhân dân"),
        # ('Đời sống', 'https://nhandan.vn/rss/bhxh-va-cuoc-song-1222.rss', "Nhân dân"),
        # ('Đời sống', 'https://nhandan.vn/rss/nguoi-tot-viec-tot-1319.rss', "Nhân dân"),
        # ('Sức khoẻ', 'https://nhandan.vn/rss/y-te-1309.rss', "Nhân dân"),
        # ('Giáo dục', 'https://nhandan.vn/rss/giaoduc-1303.rss', "Nhân dân"),
        # ('Thể thao', 'https://nhandan.vn/rss/thethao-1224.rss', "Nhân dân"),
        # ('Giải trí', 'https://nhandan.vn/rss/vanhoa-1251.rss', "Nhân dân"),
        # ('Du lịch', 'https://nhandan.vn/rss/du-lich-1257.rss', "Nhân dân"),
        # ('Pháp luật', 'https://nhandan.vn/rss/phapluat-1287.rss', "Nhân dân"),
        # ('Khoa học - Công nghệ', 'https://nhandan.vn/rss/khoahoc-congnghe-1292.rss', "Nhân dân")
    ]
]

def parseRss(rssFeed):
    data = {}
    for category, feed_url, publisher in rssFeed:
        feed = feedparser.parse(feed_url)
        for entry in feed.entries:
            image_link = ''
            if hasattr(entry, 'media_content') and entry.media_content:
                image_link = entry.media_content[0].get('url', '')
            elif hasattr(entry, 'media_thumbnail') and entry.media_thumbnail:
                image_link = entry.media_thumbnail[0].get('url', '')
            elif hasattr(entry, 'enclosure') and entry.enclosure:
                if entry.enclosure.get('type', '').startswith('image/'):
                    image_link = entry.enclosure.get('url', '')
            elif entry.get('description'):
                soup = BeautifulSoup(entry.description, 'html.parser')
                img_tag = soup.find('img')
                if img_tag and img_tag.get('src'):
                    image_link = img_tag['src']

            article = {
                "title": entry.title,
                "link": entry.link,
                "published": entry.get('published', ''),
                "summary": entry.get('description', ''),
                "source": feed_url,
                "fetched_at": datetime.utcnow().isoformat(),
                "publisher": publisher,
                "image_link": image_link 
            }

            if article['link'] not in data.keys():
                is_duplicate, matched_title = CHECKERS[category].check_duplicate(article['title'])
                if is_duplicate:
                    print(f"Tiêu đề trùng lặp với: '{matched_title}'")
                else:
                    print("Tiêu đề mới, đã thêm vào danh sách.")

                article['categories'] = [category]
                data[article['link']] = article
            else:
                data[article['link']]['categories'].append(category)

    return data

def parseData(data):
    for article in data.values():
        try:
            full_content = fetch_full_content(article['link'])
            article["full_content"] = full_content['content']
            article["title"] = full_content['title']
            article["author"] = full_content['author']
            result = articles_collection.insert_one(article.copy())
            article_id = str(result.inserted_id)
            logger.info(f"Inserted article {article_id} from {article['link']}")
            producer.send('new_articles', article)
            logger.info(f"Send article {article_id} from {article['link']}")
        except Exception as e:
            logger.error(f"Error processing feed: {str(e)}")


def fetch_full_content(url):
    try:
        return crawler.crawl_article(url)
    except Exception as e:
        logger.error(f"Error fetching content from {url}: {str(e)}")
        return ""



def crawl():
    logger.info("Start crawling")
    data = parseRss(RSS_FEEDS[0])
    parseData(data)
    logger.info("Crawling completed")

if __name__ == "__main__":
    urls = [
        # 'https://vnexpress.net/kho-bac-da-giai-ngan-hon-10-400-ty-dong-chi-phi-tang-qua-2-9-4934005.html',
        # 'https://vtcnews.vn/3-chinh-sach-giao-duc-quan-trong-co-hieu-luc-tu-thang-9-2025-ar963105.html',
        # 'https://dantri.com.vn/xa-hoi/viet-nam-trao-tang-nhan-dan-cuba-385-ty-dong-20250901135605157.htm',
        # 'https://thanhnien.vn/viet-nam-nhan-chuyen-giao-cong-nghe-san-xuat-thuoc-dieu-tri-ung-thu-cua-cuba-18525090115084312.htm',
        # 'https://vietnamnet.vn/chan-dung-13-bo-truong-gd-dt-trong-80-nam-qua-2436981.html',
        # 'https://vietnamnet.vn/an-so-o-le-duyet-binh-co-luong-may-bay-lon-nhat-cua-40-nam-truoc-2437534.html',
        # 'https://tienphong.vn/nhung-nguoi-thoi-hon-cho-cuoc-dieu-binh-lich-su-post1774647.tpo',
        "https://tuoitre.vn/nguoi-ha-noi-da-ke-khai-thong-tin-3-3-trieu-xe-co-cong-an-gap-rut-lam-sach-du-lieu-20251209220716227.htm"
    ]

    for url in urls:
        response = crawler.crawl_article(url)
        print(response)

    # crawl()
    schedule.every(1).hours.do(crawl)
    while True:
        schedule.run_pending()
        time.sleep(300)

