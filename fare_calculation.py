import dask.dataframe as dd
import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd


from pipeline import connect_to_database,read_data_from_cassandra

from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestRegressor
from xgboost import XGBRegressor
from sklearn.linear_model import Ridge
from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score

database,_ = connect_to_database('config.json')
df = read_data_from_cassandra(database,"silver_trips")

df_new = df.compute()
df_new['tpep_pickup_hour'] = df_new["tpep_pickup_datetime"].dt.hour
df_new['tpep_dropoff_hour'] = df_new["tpep_dropoff_datetime"].dt.hour

df_new.drop(["tpep_pickup_datetime","tpep_dropoff_datetime","extra", "mta_tax", "tip_amount","tolls_amount", "total_amount","improvement_surcharge","congestion_surcharge", "airport_fee"	],axis=1,inplace=True)


df_new = dd.get_dummies(df_new, columns=['PULocationID', 'DOLocationID'], drop_first=True)

X = df_new.drop('fare_amount', axis=1)
y = df_new['fare_amount']

X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

# Initialize models
models = {
    "Random Forest": RandomForestRegressor(n_estimators=100, random_state=42, n_jobs=-1),
    "XGBoost": XGBRegressor(n_estimators=100, random_state=42, n_jobs=-1),
    "Ridge Regression": Ridge(alpha=1.0)
}

# Train and evaluate
results = {}
for name, model in models.items():
    model.fit(X_train, y_train)
    y_pred = model.predict(X_test)
    
    results[name] = {
        'model': model,
        'r2': r2_score(y_test, y_pred),
        'mse': mean_squared_error(y_test, y_pred),
        'mae': mean_absolute_error(y_test, y_pred),
        'predictions': y_pred
    }
    
# Display results
metrics_df = dd.DataFrame.from_dict({k: [v['r2'], v['mse'], v['mae']] 
                                   for k, v in results.items()}, 
                                  orient='index',
                                  columns=['R2 Score', 'MSE', 'MAE'])


print(metrics_df.compute().sort_values('R2 Score', ascending=False))


# 4. Metrics Comparison
metrics_df.compute().plot(kind='bar', subplots=True, layout=(1,3), figsize=(15,5))
plt.tight_layout()
plt.show()

# 1. Actual vs Predicted Plot
plt.figure(figsize=(15, 5))
for i, (name, res) in enumerate(results.items(), 1):
    plt.subplot(1, 3, i)
    sns.scatterplot(x=y_test, y=res['predictions'], alpha=0.3)
    plt.plot([y_test.min(), y_test.max()], [y_test.min(), y_test.max()], 'k--')
    plt.title(f'{name} Predictions')
    plt.xlabel('Actual Fare')
    plt.ylabel('Predicted Fare')
plt.tight_layout()
plt.show()

# 2. Feature Importance (for tree-based models)
plt.figure(figsize=(15, 5))
for i, (name, res) in enumerate(results.items(), 1):
    if hasattr(res['model'], 'feature_importances_'):
        plt.subplot(1, 2, i)
        importances = res['model'].feature_importances_
        top_features = pd.Series(importances, index=X.columns).sort_values(ascending=False)[:10]
        sns.barplot(x=top_features.values, y=top_features.index)
        plt.title(f'{name} - Top 10 Features')
plt.tight_layout()
plt.show()


# 3. Error Distribution
plt.figure(figsize=(10, 6))
for name, res in results.items():
    errors = y_test - res['predictions']
    sns.kdeplot(errors, label=name)
plt.title('Error Distribution Comparison')
plt.xlabel('Prediction Error')
plt.legend()
plt.show()