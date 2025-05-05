import dask.dataframe as dd

from pipeline import connect_to_database, load_data_to_cassandra, clean_data,read_data_from_cassandra
database,_ = connect_to_database('config.json')
raw_df = read_data_from_cassandra(database,"bronze_trips")
clean_df = clean_data(raw_df)
load_data_to_cassandra(clean_df.compute(),database,"silver_trips", step=100)