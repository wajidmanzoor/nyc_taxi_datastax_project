import json
from astrapy import DataAPIClient
from tqdm import tqdm
import dask.dataframe as dd
import dask.bag as db

def connect_to_database(config_path):
    with open(config_path) as f:
        config = json.load(f)
        
    client = DataAPIClient(config["ASTRA_DB_TOKEN"])
    database = client.get_database_by_api_endpoint(config["ASTRA_DB_API_ENDPOINT"])
    return database,client


def load_data_to_cassandra(df,database,collection_name, step=100):
    try:
        if collection_name not in database.list_collection_names():
            print(f"\nCollection {collection_name} does not exist. Creating {collection_name}")
            database.create_collection(collection_name)
        collection = database.get_collection(collection_name)
        docs = df.to_dict(orient="records")
    except Exception as e:
         print(f"\nAn error occurred while connecting to collections : {e}")
         return 
    
    pbar = tqdm(total=len(df),desc="Data Uploaded ")
    try:
        records = []
        for i,doc in enumerate(docs):
            records.append(doc)
            if((i+1)%step==0):
                collection.insert_many(records)
                pbar.update(len(records))
                records.clear()
            elif((i+1)==len(df)):
                collection.insert_many(records)
                pbar.update(len(records))
                records.clear()
        print(f"\nData loaded into Cassandra {collection_name} Table")
        print(f"Total Columns: {len(df.columns)} Total Rows: {len(df)}")
        
    except Exception as e:
         print(f"\nAn error occurred while uploading data row {i} to {collection_name}: {e}")
    finally:
        pbar.close()
def clean_data(df, filepath="data/silver_cleaned_tripdata.csv"):
    try:
        clean_df = df.dropna()
        clean_df = clean_df[(clean_df['passenger_count'] > 0) &
                            (clean_df['trip_distance'] > 0) &
                            (clean_df['fare_amount'] > 0)]

        clean_df = clean_df[['tpep_pickup_datetime', 'tpep_dropoff_datetime', 'passenger_count',
                            'trip_distance', 'fare_amount','extra', 'mta_tax', 'tip_amount','tolls_amount','total_amount','improvement_surcharge', 'payment_type',
                            'PULocationID', 'DOLocationID','congestion_surcharge','airport_fee']]
        
        for col in ['tpep_pickup_datetime', 'tpep_dropoff_datetime']:
            if col in clean_df.columns:
                clean_df[col] = dd.to_datetime(clean_df[col], utc=True)
        
        clean_df.to_csv(filepath, index=False, single_file=True)
        print("\nData Cleaned and Saved")
        return clean_df
    except Exception as e:
        print(f"\nAn error occurred while cleaning data : {e}")
        


def read_data_from_cassandra(db_client, collection_name,npartitions=100):
    """
    Reads data from a Cassandra collection into a Dask DataFrame.

    Parameters:
    - db_client: The Astra DB client object.
    - collection_name: Name of the Cassandra collection/table.

    Returns:
    - Dask DataFrame containing the collection data.
    """
    try:
        if collection_name not in db_client.list_collection_names():
            print(f"\nCollection {collection_name} does not exist.")
            return None
        
        collection = db_client.get_collection(collection_name)
        print(f"\nReading data from {collection_name} collection...")

        # Fetch all documents as a Python list
        docs = list(collection.find({}))

        if not docs:
            print(f"\nNo documents found in {collection_name}.")
            return None
        
        # Use Dask Bag to parallelize the list
        bag = db.from_sequence(docs, npartitions=npartitions)  # You can tune partitions
        # Convert Dask Bag to Dask DataFrame
        df = bag.to_dataframe()

        print(f"Loaded {len(docs)} rows from {collection_name} into Dask DataFrame")
        return df
    
    except Exception as e:
        print(f"\nAn error occurred while reading from {collection_name}: {e}")
        return None


                