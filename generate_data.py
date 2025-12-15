import pandas as pd
import numpy as np
from datetime import datetime, timedelta

# Settings
n_rows = 1000
start_time = datetime.now()

# 1. Generate BTC (Random Walk)
np.random.seed(42)
returns_btc = np.random.normal(0, 0.001, n_rows)
price_btc = 50000 * np.exp(np.cumsum(returns_btc))

# 2. Generate ETH (Cointegrated with BTC)
# ETH = Intercept + Beta * BTC + Noise (Stationary Spread)
beta = 0.06
spread_noise = np.random.normal(0, 50, n_rows) # Mean reverting noise
price_eth = (1000 + beta * price_btc) + spread_noise

# 3. Create DataFrame
timestamps = [start_time + timedelta(minutes=i) for i in range(n_rows)]

df_btc = pd.DataFrame({'timestamp': timestamps, 'symbol': 'BTCUSDT', 'close': price_btc})
df_eth = pd.DataFrame({'timestamp': timestamps, 'symbol': 'ETHUSDT', 'close': price_eth})

# 4. Combine and Shuffle
df_final = pd.concat([df_btc, df_eth]).sort_values('timestamp')

# 5. Save
filename = "demo_market_data.csv"
df_final.to_csv(filename, index=False)

print(f"✅ Generated {filename} with {len(df_final)} rows.")
print("Upload this file to your AlphaTrawler Dashboard.")