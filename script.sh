#!/bin/bash

# Create directories
mkdir -p nyc_taxi_datastax_project/data
mkdir -p nyc_taxi_datastax_project/visuals
mkdir -p nyc_taxi_datastax_project/secure-connect-nyc-taxi-project

# Create data files
touch nyc_taxi_datastax_project/data/bronze_yellow_tripdata.csv
touch nyc_taxi_datastax_project/data/silver_cleaned_tripdata.csv

# Create visualizations
touch nyc_taxi_datastax_project/visuals/trip_count_by_passenger.png
touch nyc_taxi_datastax_project/visuals/avg_fare_by_passenger.png
touch nyc_taxi_datastax_project/visuals/tip_distribution.png

# Create script files
touch nyc_taxi_datastax_project/1_data_ingestion.py
touch nyc_taxi_datastax_project/2_data_cleaning.py
touch nyc_taxi_datastax_project/3_data_to_cassandra.py
touch nyc_taxi_datastax_project/4_visualization.py

# Create README
#touch nyc_taxi_datastax_project/README.md

echo "Project structure created successfully."
