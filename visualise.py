import pandas as pd
import matplotlib.pyplot as plt
import sqlite3

conn = sqlite3.connect('estat_data.db')
df = pd.read_sql_query('SELECT * FROM monthly_stats ORDER BY date', conn)
conn.close()

df['date'] = pd.to_datetime(df['date'], format='%Y-%m')

plt.figure(figsize=(12, 6))
plt.plot(df['date'], df['household_overall'], label='Household Overall')
plt.plot(df['date'], df['business_overall'], label='Business Overall')
plt.plot(df['date'], df['employment'], label='Employment')
plt.plot(df['date'], df['net_migration'], label='Net Migration')

plt.title('Monthly Economic Indicators')
plt.xlabel('Date')
plt.ylabel('Value')
plt.legend()
plt.grid(True)
plt.tight_layout()
plt.show()
