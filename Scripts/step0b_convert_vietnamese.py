import pandas as pd
import os

# Đường dẫn file đầu vào & đầu ra
input_path = "data/train_nor_811.xlsx"     # file excel của bạn
output_path = "Output/step0/vietnamese_sentiment.csv"

os.makedirs("Output/step0", exist_ok=True)

# Đọc Excel
df = pd.read_excel(input_path)

# Xuất CSV (UTF-8 để tránh lỗi tiếng Việt)
df.to_csv(output_path, index=False, encoding='utf-8-sig')

print("Đã chuyển Excel thành CSV xong!")
print("Lưu tại:", output_path)
