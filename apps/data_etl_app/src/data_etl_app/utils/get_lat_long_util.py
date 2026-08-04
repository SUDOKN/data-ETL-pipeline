import requests
import pandas as pd

df = pd.read_csv("csv2_updated-5.csv", encoding="latin1")
counter = 0


def get_lat_long(api_key, zip_code):
    url = f"https://api.geocod.io/v1.6/geocode?postal_code={zip_code}&api_key={api_key}"
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        if "results" in data and len(data["results"]) > 0:
            location = data["results"][0]["location"]
            print(location)
            return location["lat"], location["lng"]
    return None, None


api_key = ""
zip_code = "85756-7002"
# latitude, longitude = get_lat_long(api_key, zip_code)

for index, row in df.iterrows():
    # for i in range(1, 9):
    #     zip_code = row[f'Zip_{i}']
    #     print(zip_code)

    #     if pd.notna(zip_code) and not pd.notna(row[f'Latitude_{i}']):
    #         # print(zip_code)
    #         # print(row[f'Latitude_{i}'])
    #         lat, long = get_lat_long(api_key, zip_code)

    #         # Update DataFrame
    #         df.at[index, f'Latitude_{i}'] = lat
    #         df.at[index, f'Longitude_{i}'] = long
    #         df.to_csv('db_tranformed.csv', index=False)

    if row["Zip_1"] and row["State_1"] == "Arizona" and str(row["Latitude_1"]) == "nan":
        zip_code = row["Zip_1"]
        print(zip_code)
        lat, long = get_lat_long(api_key, zip_code)
        df.at[index, f"Latitude_1"] = lat
        df.at[index, f"Longitude_1"] = long
        df.to_csv("csv2_updated-6.csv", index=False)
        print(counter)
        counter += 1
