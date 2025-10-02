from bs4 import BeautifulSoup
import re

class Cleaner:
    REMOVE_PATTERN = re.compile(
        r"(^side$|combx|retweet|mediaarticlerelated|menucontainer|"
        r"nav|navbar|storytopbar|utility-bar|inline-share-tools|comment|"
        r"PopularQuestions|contact|foot|footer|Footer|footnote|"
        r"cnn_strycaptiontxt|cnn_html_slideshow|cnn_strylftcntnt|"
        r"links|shoutbox|sponsor|tags|socialnetworking|"
        r"socialNetworking|cnnStryHghLght|cnn_stryspcvbx|^inset$|"
        r"pagetools|post-attributes|subscribe|vcard|articleheadings|"
        r"date|^print$|popup|author-dropdown|tools|socialtools|byline|"
        r"breadcrumbs|wp-caption-text|legende|ajoutVideo|timestamp|js_replies)",
        re.I
    )

    def clean_html(self, html):
        soup = BeautifulSoup(html, 'html.parser')

        # 1. Loại bỏ các thẻ không mong muốn
        for tag in soup(['script', 'style', 'header', 'footer', 'nav', 'aside', 'iframe', 'form']):
            tag.decompose()

        # 2. Loại bỏ theo id/class/name
        for attr in ['id', 'class', 'name']:
            for tag in soup.find_all(attrs={attr: self.REMOVE_PATTERN}):
                tag.decompose()

        # 3. Loại bỏ quảng cáo theo class thường gặp
        for tag in soup.find_all(
            ['div', 'section'],
            class_=re.compile(r'(^|[-_\s])(ad|advert|banner|sponsor)([-_\s]|$)', re.I)
        ):
            tag.decompose()

        # 4. Loại bỏ <em> không chứa ảnh
        for em in soup.find_all('em'):
            if not em.find('img'):
                em.unwrap()

        # 5. Loại bỏ drop-cap
        for span in soup.select('span.dropcap, span.drop_cap'):
            span.decompose()

        # 6. Loại bỏ span trong <p>
        for span in soup.select('p span'):
            span.unwrap()

        # 7. Xóa thẻ rỗng (trừ img, br)
        for tag in soup.find_all():
            if not tag.get_text(strip=True) and tag.name not in ['img', 'br']:
                tag.decompose()

        # 8. Xử lý body
        body = soup.find('body')
        if body:
            if 'class' in body.attrs:
                del body['class']
            return str(body)
        else:
            raise Exception("No <body> tag found in HTML")
