import pandas as pd
import numpy as np
import os
from checker import TitleDuplicateChecker

TOPICS = [
    'Xã hội', 'Thế giới', 'Kinh tế', 'Đời sống', 'Sức khoẻ',
    'Giáo dục', 'Thể thao', 'Giải trí', 'Du lịch',
    'Pháp luật', 'Khoa học - Công nghệ', 'Xe'
]

def read_all_csvs_to_tuples():
    all_tuples = []
    
    for topic in TOPICS:
        file_path = f'datasets/{topic.lower()}.csv'
        if not os.path.exists(file_path):
            print(f"File không tồn tại: {file_path}")
            continue
        
        try:
            df = pd.read_csv(file_path, encoding='utf-8', header=None)
            tuples = list(zip(df[0], df[1]))
            all_tuples.extend(tuples)
            
            print(f"{topic}: {len(tuples)} tuples")
            
        except Exception as e:
            print(f"Lỗi đọc {topic}: {e}")

    result_array = np.array(all_tuples)
    
    print(f"\nTỔNG KẾT:")
    print(f"  - Tổng số tuples: {len(result_array)}")
    print(f"  - Shape của array: {result_array.shape}")
    return result_array


def precision(threshold, data, log_interval=50):
    correct = 0
    checker = TitleDuplicateChecker(similarity_threshold=threshold)

    print(f"\nĐang kiểm tra threshold = {threshold:.2f} ...")
    for i, (title1, title2) in enumerate(data, start=1):
        is_duplicate1, matched_title1 = checker.check_duplicate(title1)
        if is_duplicate1:
            continue

        is_duplicate2, matched_title2 = checker.check_duplicate(title2)
        if is_duplicate2 and matched_title2 == title1:
            correct += 1

        if i % log_interval == 0:
            print(f" Đã xử lý {i}/{len(data)} cặp - đúng: {correct}")

    precision_value = correct / len(data)
    print(f"Hoàn tất threshold={threshold:.2f} → precision={precision_value:.4f}")
    return precision_value


if __name__ == "__main__":
    data = read_all_csvs_to_tuples()
    thresholds = [0.05 * i for i in range(21)]

    results = []
    for t in thresholds:
        p = precision(t, data)
        results.append((t, p))
    
    output_path = "results.txt"
    with open(output_path, "w", encoding="utf-8") as f:
        f.write("Threshold\tPrecision\n")
        for t, p in results:
            f.write(f"{t:.2f}\t{p:.4f}\n")

    print(f"\n📁 Đã lưu kết quả vào: {output_path}")
