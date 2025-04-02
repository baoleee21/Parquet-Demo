import pandas as pd

# Tạo DataFrame mẫu
data = {
    "id": [1, 2, 3, 4, 5],
    "name": ["Alice", "Bob", "Charlie", "David", "Eve"],
    "age": [25, 30, 35, 40, 45]
}
df = pd.DataFrame(data)

# Lưu dưới định dạng Parquet
df.to_parquet("input.parquet", engine="pyarrow")
