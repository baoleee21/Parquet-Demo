import pandas as pd

# Đọc file Parquet vào DataFrame
df = pd.read_parquet("large_fastparquet.parquet", engine="fastparquet")

print(df)