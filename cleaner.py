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

        for tag in soup(['script', 'style', 'header', 'footer', 'nav', 'aside', 'iframe', 'form']):
            tag.decompose()

        for attr in ['id', 'class', 'name']:
            for tag in soup.find_all(attrs={attr: self.REMOVE_PATTERN}):
                tag.decompose()

        for tag in soup.find_all(
            ['div', 'section'],
            class_=re.compile(r'(^|[-_\s])(ad|advert|banner|sponsor)([-_\s]|$)', re.I)
        ):
            tag.decompose()

        for em in soup.find_all('em'):
            if not em.find('img'):
                em.unwrap()

        for span in soup.select('span.dropcap, span.drop_cap'):
            span.decompose()

        for span in soup.select('p span'):
            span.unwrap()

        for tag in soup.find_all():
            if not tag.get_text(strip=True) and tag.name not in ['img', 'br']:
                tag.decompose()

        body = soup.find('body')
        if body:
            if 'class' in body.attrs:
                del body['class']
            return str(body)
        else:
            raise Exception("No <body> tag found in HTML")
