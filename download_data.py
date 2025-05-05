import pandas as pd

data_url = 'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-01.parquet'
df_raw = pd.read_parquet(data_url)

# Limit as the data is too large
raw_df = df_raw.head(150000)
raw_df.to_csv("data/bronze_yellow_tripdata.csv", index=False)

print("[Downloaded Data ] Rows:", raw_df.shape[0], ", Columns:", raw_df.shape[1])