# Sử dụng thuật toán pandas
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import numpy as np

# Tạo DataFrame lớn với 1 triệu dòng
num_rows = 1_000_000
df = pd.DataFrame({
    "id": np.arange(num_rows),
    "name": np.random.choice(["Alice", "Bob", "Charlie", "David"], num_rows),
    "age": np.random.randint(18, 60, num_rows),
    "salary": np.random.uniform(30000, 100000, num_rows)
})

# Chuyển DataFrame thành Table của PyArrow
table = pa.Table.from_pandas(df)

# Ghi dữ liệu vào file Parquet với compression (để tiết kiệm dung lượng)
pq.write_table(table, "large_data.parquet", compression="snappy")

print("✅ Data has been written to large_data.parquet")
