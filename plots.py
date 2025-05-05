import dask.dataframe as dd
from pipeline import connect_to_database,read_data_from_cassandra
import matplotlib.pyplot as plt
import numpy as np
import seaborn as sns


database,_ = connect_to_database('config.json')

summary = read_data_from_cassandra(database,"gold_distribution_data")
summary.index = summary.statistics
summary = summary.drop(['_id','statistics'], axis=1)

X = summary.columns
mean = np.array(summary.loc['mean'])[0]
std = np.array(summary.loc['std'])[0]
  
X_axis = np.arange(len(X)) 
fig = plt.figure(figsize=(30,10))
plt.bar(X_axis - 0.2, mean, 0.4, label = 'mean') 
plt.bar(X_axis + 0.2, std, 0.4, label = 'std') 
  
plt.xticks(X_axis, X) 
plt.xlabel("Feature Name") 
plt.xticks(rotation=45, ha='right')
plt.ylabel("Mean/Std") 
plt.title("Mean and Std of each Feature") 
plt.legend() 
plt.savefig("visuals/Mean Std of Features.png", dpi=300)



df = read_data_from_cassandra(database,"silver_trips")

payment_type_counts =  df['payment_type'].compute().value_counts()

fig, ax = plt.subplots(figsize=(7, 7))
ax.pie(payment_type_counts, labels=payment_type_counts.index, autopct='%1.1f%%')

ax.set_title('Payment Type Distribution')

plt.savefig("visuals/Payment Type Pie Chart.png",dpi=300)


columns_to_plot = ['trip_distance', 'fare_amount', 'tip_amount','passenger_count']
names = ["Trip Distance","Fare Ammount", "Trip Amount","Passenger Count"]
fig,ax =  plt.subplots(2,2,figsize=(6,6))
axs = ax.flatten()
for i,col in enumerate(columns_to_plot):
    axs[i].hist(df[col].compute(), bins=50, alpha=0.6, label=col)
    axs[i].set_xlabel(names[i])
    axs[i].set_ylabel("frequency")

plt.suptitle('Distribution Plots')
plt.legend()
plt.tight_layout()
plt.savefig("visuals/Distribution Plots of Features.png",dpi=300)


daily_trips = read_data_from_cassandra(database,"gold_hourly_trips")

plt.bar(daily_trips['tpep_pickup_hour'].compute(), daily_trips['trip_count'].compute(),label="Trip Count")
plt.legend()
plt.xlabel("Hours")
plt.ylabel("Trip Count")
plt.savefig("visuals/Hourly Trip Count.png",dpi=300) 

correlation_matrix = read_data_from_cassandra(database,"gold_corelation_data")

correlation_matrix.index = correlation_matrix['columns']
correlation_matrix = correlation_matrix.drop(['_id','columns'], axis=1)

plt.figure(figsize=(10, 8))
sns.heatmap(correlation_matrix.compute(), annot=True, cmap='coolwarm', fmt='.2f', cbar=True, linewidths=0.5)

plt.title('Correlation Matrix of Selected Columns')
plt.savefig("visuals/Correlation Heatmap.png",dpi=300)