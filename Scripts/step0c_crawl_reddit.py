import requests
import pandas as pd
import os


def crawl_reddit(keyword="google", limit=300):
    # 1. Gọi API
    url = f"https://api.pullpush.io/reddit/search?query={keyword}&size={limit}"
    print(f"[INFO] Gọi API: {url}")
    resp = requests.get(url, timeout=15)

    print(f"[INFO] HTTP status = {resp.status_code}")
    resp.raise_for_status()  # nếu lỗi 4xx/5xx sẽ báo ngay

    # 2. Parse JSON
    try:
        payload = resp.json()
    except Exception as e:
        print("[ERROR] Không parse được JSON:", e)
        print("Response text sample:", resp.text[:500])
        return pd.DataFrame(columns=["source", "text"])

    # 3. Lấy danh sách item
    if isinstance(payload, dict):
        items = payload.get("data", [])
    elif isinstance(payload, list):
        items = payload
    else:
        items = []

    print(f"[INFO] Số phần tử nhận được từ API: {len(items)}")

    # 4. Lọc lấy text
    data = []
    for item in items:
        if not isinstance(item, dict):
            continue

        text = (
            item.get("body")
            or item.get("selftext")
            or item.get("title")
            or item.get("text", "")
        )

        if text and len(text) > 8:
            data.append({"source": "reddit", "text": text})

    print(f"[INFO] Số dòng hợp lệ sau khi lọc: {len(data)}")

    return pd.DataFrame(data)


if __name__ == "__main__":
    # 5. Thiết lập đường dẫn lưu file
    BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    RAW_DIR = os.path.join(BASE_DIR, "data", "raw")
    os.makedirs(RAW_DIR, exist_ok=True)

    # 6. Crawl
    df = crawl_reddit(keyword="google", limit=300)

    # 7. Nếu không có dữ liệu thì báo
    if df.empty:
        print("[WARN] Không crawl được bản ghi nào. Kiểm tra lại API / mạng / keyword.")
    else:
        output_path = os.path.join(RAW_DIR, "reddit_raw.csv")
        df.to_csv(output_path, index=False, encoding="utf-8")
        print(f"[OK] Đã lưu {len(df)} dòng vào: {output_path}")
