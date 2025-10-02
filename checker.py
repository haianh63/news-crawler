from sentence_transformers import SentenceTransformer
import numpy as np
import faiss
from underthesea import word_tokenize
import re

class TitleDuplicateChecker:
    def __init__(self, similarity_threshold=0.7):
        self.model = SentenceTransformer('distiluse-base-multilingual-cased-v2')
        self.similarity_threshold = similarity_threshold
        self.crawled_titles = []
        self.index = None
        self.dimension = None

    def preprocess_title(self, title):
        title = title.lower()
        title = re.sub(r'[^\w\s]', '', title)
        return word_tokenize(title, format="text")

    def fit(self, titles):
        self.crawled_titles = list(titles)
        if not self.crawled_titles:
            return
        processed_titles = [self.preprocess_title(t) for t in self.crawled_titles]
        vectors = self.model.encode(processed_titles, convert_to_numpy=True).astype('float32')
        self.dimension = vectors.shape[1]
        self.index = faiss.IndexFlatIP(self.dimension)
        faiss.normalize_L2(vectors)
        self.index.add(vectors)

    def check_duplicate(self, new_title):
        if self.index is None:
            self.fit([new_title])
            return False, None
        processed_title = self.preprocess_title(new_title)
        new_vec = self.model.encode([processed_title], convert_to_numpy=True).astype('float32')
        faiss.normalize_L2(new_vec)
        distances, indices = self.index.search(new_vec, 1)
        if distances[0][0] >= self.similarity_threshold:
            return True, self.crawled_titles[indices[0][0]]
        self.index.add(new_vec)
        self.crawled_titles.append(new_title)
        return False, None

    def add_titles(self, titles):
        """
        Thêm nhiều tiêu đề mới và cập nhật index.
        
        Args:
            titles (list): Danh sách tiêu đề mới để thêm.
        """
        for title in titles:
            is_duplicate, _ = self.check_duplicate(title)
            if is_duplicate:
                print(f"Tiêu đề '{title}' trùng lặp, bỏ qua.")
            else:
                print(f"Thêm tiêu đề mới: '{title}'")