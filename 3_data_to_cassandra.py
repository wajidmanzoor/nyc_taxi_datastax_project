from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from uuid import uuid4

SECURE_BUNDLE_PATH = "./secure-connect-nyc-taxi-project"  # <- Replace with your secure bundle path
ASTRA_DB_USERNAME = "your_username"  # <- Replace this
ASTRA_DB_PASSWORD = "your_password"  # <- Replace this

cloud_config = {
    'secure_connect_bundle': SECURE_BUNDLE_PATH
}

auth_provider = PlainTextAuthProvider(ASTRA_DB_USERNAME, ASTRA_DB_PASSWORD)
cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider)
session = cluster.connect()
session.set_keyspace('nyc_taxi')

session.execute("""
    CREATE TABLE IF NOT EXISTS yellow_trips (
        trip_id UUID PRIMARY KEY,
        pickup_datetime timestamp,
        dropoff_datetime timestamp,
        passenger_count int,
        trip_distance float,
        fare_amount float,
        tip_amount float,
        payment_type text,
        pu_location_id int,
        do_location_id int
    );
""")

# Load cleaned data
df = pd.read_csv("data/silver_cleaned_tripdata.csv", parse_dates=['tpep_pickup_datetime', 'tpep_dropoff_datetime'])

insert_stmt = session.prepare("""
    INSERT INTO yellow_trips (trip_id, pickup_datetime, dropoff_datetime, passenger_count, 
    trip_distance, fare_amount, tip_amount, payment_type, pu_location_id, do_location_id) 
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
""")

for _, row in df.iterrows():
    session.execute(insert_stmt, (
        uuid4(),
        row['tpep_pickup_datetime'].to_pydatetime(),
        row['tpep_dropoff_datetime'].to_pydatetime(),
        int(row['passenger_count']),
        float(row['trip_distance']),
        float(row['fare_amount']),
        float(row['tip_amount']),
        row['payment_type'],
        int(row['PULocationID']),
        int(row['DOLocationID'])
    ))

print("[DataStax Astra] Data loaded into Cassandra")