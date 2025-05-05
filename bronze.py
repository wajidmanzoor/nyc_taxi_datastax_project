import dask.dataframe as dd

from pipeline import connect_to_database, load_data_to_cassandra

database,_ = connect_to_database('config.json')

raw_df = dd.read_csv("data/bronze_yellow_tripdata.csv", assume_missing=True)
load_data_to_cassandra(raw_df.compute(),database,"bronze_trips")