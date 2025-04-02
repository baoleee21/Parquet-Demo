import pandas as pd
import numpy as np
from fastparquet import write

# Tạo DataFrame lớn
num_rows = 1_000_000
df = pd.DataFrame({
    "id": np.arange(num_rows),
    "name": np.random.choice(["Alice", "Bob", "Charlie", "David"], num_rows),
    "age": np.random.randint(18, 60, num_rows),
    "salary": np.random.uniform(30000, 100000, num_rows)
})

# Chia batch
batch_size = 100_000
filename = "large_fastparquet.parquet"

# Ghi batch đầu tiên (tạo file)
write(filename, df.iloc[:batch_size], compression="snappy")

# Ghi các batch tiếp theo (append vào file)
for i in range(batch_size, num_rows, batch_size):
    batch_df = df.iloc[i:i+batch_size]
    write(filename, batch_df, compression="snappy", append=True)

print("✅ Large DataFrame has been written in batches using FastParquet")
