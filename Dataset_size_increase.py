# checking the dataset size

import pandas as pd
import os

file_path = "StudentPerformanceFactors.csv"

df = pd.read_csv(file_path)

file_size_mb = os.path.getsize(file_path) / (1024 ** 2)

print("Original rows:", len(df))
print("Original columns:", len(df.columns))
print("Original size:", round(file_size_mb, 2), "MB")

# Increasing the size

import pandas as pd
import os
import math

# ==========================================
# 1. LOAD ORIGINAL DATASET
# ==========================================

input_file = "StudentPerformanceFactors.csv"

df = pd.read_csv(input_file)

original_size = os.path.getsize(input_file)

print("Original rows:", len(df))
print("Original size MB:", original_size / (1024 ** 2))

# ==========================================
# 2. CREATE OUTPUT DIRECTORY
# ==========================================

os.makedirs("scaled_datasets", exist_ok=True)

# ==========================================
# 3. DEFINE TARGET SIZES
# ==========================================

target_sizes_mb = [500, 1024, 2048, 5120]

# ==========================================
# 4. GENERATE DATASETS
# ==========================================

for target_mb in target_sizes_mb:

    target_bytes = target_mb * 1024 * 1024

    # Estimate number of copies
    copies = math.ceil(target_bytes / original_size)

    print(f"\nGenerating {target_mb} MB dataset...")
    print("Estimated copies:", copies)

    output_file = (
        f"scaled_datasets/student_{target_mb}mb.csv"
    )

    # Write header and original data, then append copies
    first = True

    for replica in range(copies):

        temp = df.copy()

        # Unique identifier for each replica
        temp["_replica_id"] = replica

        temp.to_csv(
            output_file,
            mode="w" if first else "a",
            header=first,
            index=False
        )

        first = False

        # Check size and stop if target is reached
        if os.path.getsize(output_file) >= target_bytes:
            break

    actual_size = os.path.getsize(output_file) / (1024 ** 2)

    print("Generated file:", output_file)
    print("Actual size MB:", round(actual_size, 2))
    print("Completed!")

print("\nAll datasets generated!")

# gcloud storage cp scaled_datasets/*.csv \
# gs://YOUR_BUCKET/data/