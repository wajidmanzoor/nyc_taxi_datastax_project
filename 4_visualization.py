import matplotlib.pyplot as plt
import pandas as pd

# Reload cleaned data for plotting
df = pd.read_csv("data/silver_cleaned_tripdata.csv")

# Trip count by passenger count
df['passenger_count'].value_counts().sort_index().plot(kind='bar')
plt.title('Trip Count by Passenger Count')
plt.xlabel("Passenger Count")
plt.ylabel("Trip Count")
plt.savefig("visuals/trip_count_by_passenger.png")
plt.clf()

# Average fare by passenger count
df.groupby('passenger_count')['fare_amount'].mean().plot(kind='bar', color='orange')
plt.title('Average Fare by Passenger Count')
plt.xlabel("Passenger Count")
plt.ylabel("Average Fare ($)")
plt.savefig("visuals/avg_fare_by_passenger.png")
plt.clf()

# Tip amount distribution
plt.hist(df['tip_amount'], bins=50, color='green')
plt.title('Tip Amount Distribution')
plt.xlabel("Tip Amount ($)")
plt.ylabel("Frequency")
plt.savefig("visuals/tip_distribution.png")

print("[Gold Layer] Visualizations saved to /visuals")