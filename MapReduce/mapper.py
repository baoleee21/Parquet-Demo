#!/usr/bin/env python3
import sys
import pandas as pd

# Đọc file Parquet từ HDFS
def read_parquet(file_path):
    df = pd.read_parquet(file_path, engine="pyarrow")
    return df

if __name__ == "__main__":
    for line in sys.stdin:
        parquet_file = line.strip()  # Đọc file từ stdin
        try:
            df = read_parquet(parquet_file)
            for _, row in df.iterrows():
                print(f"{row['name']}\t{row['age']}")  # Xuất theo dạng key-value
        except Exception as e:
            print(f"Error reading {parquet_file}: {e}", file=sys.stderr)
