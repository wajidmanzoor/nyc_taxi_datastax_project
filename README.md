# NYC Taxi Data Processing Project using DataStax Astra (Cassandra)

## Overview
This project processes NYC Yellow Taxi trip data by creating a complete data pipeline using **Cassandra** and **Python**.  
It follows a **Medallion (Bronze-Silver-Gold)** architecture to structure the data and applies **machine learning** to predict taxi fares.


## Steps to Run

1. **Download the Dataset**  
   Run `download_data.py` to download the NYC Yellow Taxi dataset.

2. **Load Raw Data into Cassandra (Bronze Layer)**  
   Run `bronze.py` to load the raw CSV data into a Cassandra collection.

3. **Clean and Transform Data (Silver Layer)**  
   Run `silver.py` to clean the data (handle missing values, fix types) and store the cleaned version back into Cassandra.

4. **Extract Gold Datasets (Gold Layer)**  
   Run `gold.py` to create and load the following datasets into Cassandra:
   - **Gold Distribution Data:**  
     Statistical summary of each numerical feature (mean, standard deviation, min, max, percentiles, etc.).
   - **Gold Hourly Trip Count:**  
     Total number of trips aggregated for each hour of the day.
   - **Gold Correlation Data:**  
     Correlation matrix between all numerical features of the cleaned dataset.

5. **Generate Visualizations**  
   Run `plots.py` to create the following plots:
   - Bar plots showing mean and standard deviation for each feature.
   - Pie charts for the distribution of payment types.
   - Histograms showing distributions of all numerical features.
   - Bar plot of trip counts per hour of the day.
   - Heatmap of feature correlations.

6. **Fare Prediction using Machine Learning**  
   Run `fare_calculation.py` to train and evaluate ML models for fare prediction:
   - Feature selection based on the correlation matrix.
   - Model training with **Random Forest**, **XGBoost**, and **Ridge Regression**.
   - Model evaluation using **R² Score**, **MSE**, and **MAE**.
   - Visualizations:
     - Bar plot comparing evaluation metrics across models.
     - Scatter plots of actual vs predicted fare amounts.
     - Feature importance plots (for tree-based models).
     - Error distribution plots for each model.



## Project Structure

- **pipeline.py**  
  Contains utility functions to:
  - Connect to Cassandra database
  - Load data into Cassandra
  - Clean and preprocess data
  - Retrieve data from Cassandra
  
- **visuals/**  
  Stores generated plots and graphs.

- **data/**  
  Stores local copies of intermediate datasets (for backup and inspection).

- **model_results/**
  Stores the model result visualizations

- **config.json**  
  Contains database connection details for DataStax Astra Cassandra.


## Notes
- Ensure that the Cassandra collections are created before running the scripts, or the scripts will create them automatically when needed.
- Adjust `config.json` if your database credentials or Astra DB endpoint change.


