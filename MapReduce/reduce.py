#!/usr/bin/env python3
import sys

age_sum = 0
count = 0

for line in sys.stdin:
    try:
        name, age = line.strip().split("\t")
        age_sum += int(age)
        count += 1
    except ValueError:
        continue

if count > 0:
    print(f"Average Age: {age_sum / count}")
#  Nhận input từ mapper.py.
# Tính trung bình tuổi và in kết quả

