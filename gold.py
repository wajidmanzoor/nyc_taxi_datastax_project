import dask.dataframe as dd


from pipeline import connect_to_database, load_data_to_cassandra,read_data_from_cassandra
database,_ = connect_to_database('config.json')

df = read_data_from_cassandra(database,"silver_trips")

summary = df.describe().compute()
summary.drop(['DOLocationID','PULocationID','payment_type'],axis=1,inplace=True)
summary["statistics"] = summary.index
load_data_to_cassandra(summary,database,"gold_distribution_data", step=100)

df["tpep_pickup_hour"] = df["tpep_pickup_datetime"].dt.hour
daily_trips = df.groupby('tpep_pickup_hour').size().compute().reset_index(name='trip_count')
daily_trips.to_csv("data/gold_hourly_trip_count.csv", index=False)
load_data_to_cassandra(daily_trips,database,"gold_hourly_trips", step=100)


columns_to_analyze = [
    "passenger_count", "trip_distance", "fare_amount", "extra", "mta_tax", 
    "tip_amount", "tolls_amount", "total_amount", "improvement_surcharge", 
    "congestion_surcharge", "airport_fee"
]

correlation_matrix = df[columns_to_analyze].compute().corr()
correlation_matrix['columns'] = correlation_matrix.index
load_data_to_cassandra(correlation_matrix,database,"gold_corelation_data", step=100)