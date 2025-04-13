clean_df = raw_df.dropna()
clean_df = clean_df[(clean_df['passenger_count'] > 0) &
                    (clean_df['trip_distance'] > 0) &
                    (clean_df['fare_amount'] > 0)]

clean_df = clean_df[['tpep_pickup_datetime', 'tpep_dropoff_datetime', 'passenger_count',
                     'trip_distance', 'fare_amount', 'tip_amount', 'payment_type',
                     'PULocationID', 'DOLocationID']]

clean_df.to_csv("data/silver_cleaned_tripdata.csv", index=False)
print("[Silver Layer] Cleaned Rows:", clean_df.shape[0])