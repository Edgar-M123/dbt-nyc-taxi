import pandas as pd

df = pd.read_parquet("./data_files/yellow_tripdata_2025-01.parquet")

print(df.head())
