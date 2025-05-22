import pandas as pd
from pathlib import Path

# Create data/raw directory if needed
Path("data/raw").mkdir(parents=True, exist_ok=True)

# --- Cape Town Sample Data ---
cape_town_data = [...]  # Your CT data here

df_ct = pd.DataFrame(cape_town_data)
df_ct.to_csv("data/raw/cape_town_listings.csv", index=False)  # Updated path

# --- Johannesburg Sample Data --- 
jhb_data = [...]  # Your JHB data here

df_jhb = pd.DataFrame(jhb_data)
df_jhb.to_csv("data/raw/johannesburg_listings.csv", index=False)  # Updated path

print("✅ Sample data created in data/raw/")