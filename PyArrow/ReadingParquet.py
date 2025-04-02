import pandas as pd
import pyarrow.parquet as pq

# Đọc file Parquet vào DataFrame
df = pd.read_parquet("large_data.parquet", engine="pyarrow")

print(df)
