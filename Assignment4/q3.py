from pymongo import MongoClient
import pandas as pd
import requests

# Connect to MongoDB server
client = MongoClient("mongodb://localhost:27017/")

# Create a database
db = client["assignment4"]


def process(chunk):
    db.TaxiTrips.insert_many(chunk.to_dict('records'))


count = db.TaxiTrips.count_documents({})
if count == 0:
    chunksize = 10 ** 8
    for chunk in pd.read_csv("E:\\UNB\\GGE6505\\Assignments\\Taxi_Trips.csv", chunksize=chunksize, low_memory=False):
        process(chunk)
else:
    print("some data exists!")

# Get the most updated version of data
url = "https://data.cityofchicago.org/resource/wrvz-psew.json"
params = {
    "$order": "trip_start_timestamp DESC",
    "$limit": 1000  # Number of records to retrieve in each request
}
offset = 0  # Initial offset

while True:
    # Add offset parameter to API request
    params["$offset"] = offset

    # Retrieve data from API
    response = requests.get(url, params=params)
    if response.status_code == 200:
        trips = response.json()

        # Check if any of the returned data doesn't exist in MongoDB
        existing_trips = db.TaxiTrips.find({"Trip ID": {"$in": [t["trip_id"] for t in trips]}})
        existing_trip_ids = set([t["Trip ID"] for t in existing_trips])
        new_trips = [t for t in trips if t["trip_id"] not in existing_trip_ids]

        # Insert new data into MongoDB
        if new_trips:
            db.TaxiTrips.insert_many(new_trips)
            print(f"{len(new_trips)} new trips inserted into MongoDB")

        # Check if there are more records to retrieve
        if len(trips) < params["$limit"]:
            print("All records retrieved")
            break

        # Update offset for next request
        offset += params["$limit"]
    else:
        print("Failed to retrieve data from API")
        break

# Now the data is up-to-date
# Queries
# 1. Query trips with a tip greater than $300:
docs = db.TaxiTrips.find({"Tips": {"$gt": 300}})
print("1. Query trips with a tip greater than $300:")
for doc in docs:
    print(doc)

# 2. Get the number of trips per year:

pipeline = [
    {"$group": {"_id": "$company", "num_trips": {"$sum": 1}}}
]

results = db.TaxiTrips.aggregate(pipeline)
print("2. Get the number of trips per year:")
for result in results:
    print(f"{result['_id']}: {result['num_trips']} trips")

# 3. Get the top 10 most popular pickup community areas:
pipeline = [
    {
        '$group': {
            '_id': '$Pickup Community Area',
            'count': {'$sum': 1}
        }
    },
    {
        '$sort': {'count': -1}
    },
    {
        '$limit': 10
    }
]

result = db.TaxiTrips.aggregate(pipeline)
print("3. Get the top 10 most popular pickup community areas:")
for r in result:
    print(r['_id'], r['count'])

# 4. Getting the top 10 pickup community area by count:
pipeline = [
    {
        '$group': {
            '_id': {'Pickup Community Area': '$Pickup Community Area'},
            'count': {'$sum': 1}
        }
    },
    {'$sort': {'count': -1}},
    {'$limit': 10}
]

result = db.TaxiTrips.aggregate(pipeline)
print("4. Getting the top 10 pickup community area by count:")
for doc in result:
    print(doc)
