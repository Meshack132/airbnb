import pandas as pd

# --- Cape Town Listings ---
cape_town_data = [
    {
        "id": 1001,
        "name": "Seaside Apartment in Muizenberg",
        "host_name": "John",
        "neighbourhood": "Muizenberg",
        "room_type": "Entire home/apt",
        "price": 850,
        "number_of_reviews": 42,
        "availability_365": 180
    },
    {
        "id": 1002,
        "name": "Cozy Studio in City Bowl",
        "host_name": "Mary",
        "neighbourhood": "City Bowl",
        "room_type": "Private room",
        "price": 500,
        "number_of_reviews": 58,
        "availability_365": 200
    },
    {
        "id": 1003,
        "name": "Camps Bay Villa with View",
        "host_name": "Richard",
        "neighbourhood": "Camps Bay",
        "room_type": "Entire home/apt",
        "price": 2500,
        "number_of_reviews": 12,
        "availability_365": 365
    }
]

df_ct = pd.DataFrame(cape_town_data)
df_ct.to_csv("cape_town_listings.csv", index=False)
print("✅ Created 'cape_town_listings.csv'")

# --- Johannesburg Listings ---
jhb_data = [
    {
        "id": 2001,
        "name": "Sandton High-Rise Apartment",
        "host_name": "Lerato",
        "neighbourhood": "Sandton",
        "room_type": "Entire home/apt",
        "price": 1200,
        "number_of_reviews": 33,
        "availability_365": 150
    },
    {
        "id": 2002,
        "name": "Braamfontein Budget Room",
        "host_name": "Themba",
        "neighbourhood": "Braamfontein",
        "room_type": "Shared room",
        "price": 300,
        "number_of_reviews": 76,
        "availability_365": 220
    },
    {
        "id": 2003,
        "name": "Maboneng Loft",
        "host_name": "Ayanda",
        "neighbourhood": "Maboneng",
        "room_type": "Entire home/apt",
        "price": 700,
        "number_of_reviews": 21,
        "availability_365": 365
    }
]

df_jhb = pd.DataFrame(jhb_data)
df_jhb.to_csv("johannesburg_listings.csv", index=False)
print("✅ Created 'johannesburg_listings.csv'")
