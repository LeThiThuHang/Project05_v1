import json
from pymongo import MongoClient

#connect
client = MongoClient("mongodb://localhost:27017/")
db = client["Unigap_Project04"]
collection = db["Products_full"]

# Set the output file prefix and initialize a counter
output_file_prefix = "split_files/mongodb_10K"
counter = 1

# Define the batch size (number of documents per file)
batch_size = 10000

# Initialize the skip variable
skip = 0

while True:
    # Query the collection and limit the results to the batch size
    cursor = collection.find({}, projection={"_id": 0}).skip(skip).limit(batch_size)

    #Check if there are any more documents to fetch -- NEED TO DO
    # Create a list to store the batch of documents
    batch = []
    for doc in cursor:
        batch.append(doc)

    # Generate the output file name with the prefix and counter
    output_file_name = f"{output_file_prefix}_{counter}.json"
    
    # Write the batch of documents to a JSON file
    with open(output_file_name, "w", encoding="utf-8") as output_file:
        json.dump(batch, output_file, ensure_ascii=False, indent=2)
    
    # Increment the counter and skip value for the next batch
    counter += 1
    skip += batch_size

