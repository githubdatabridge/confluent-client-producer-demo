import pandas as pd
from faker import Faker
import random
import time
import json

fake = Faker()

def generate_vehicle_location(vehicle_id):
    latitude = round(random.uniform(47.3645, 47.4155), 6)           # Latitude for Zurich
    longitude = round(random.uniform(8.5004, 8.5687), 6)        # Longitude for Zurich
    timestamp = int(time.time())
    
    return {
        "vehicle_id": vehicle_id,
        "location": {"latitude": latitude, "longitude": longitude},
        "ts": timestamp
    }

def write_to_json_file(data, file_path):
    with open(file_path, 'a') as json_file:
        json.dump(data, json_file)
        json_file.write('\n')

def continuously_stream_and_write_vehicle_data(output_directory, interval_seconds=1, records_per_file=1000):
    vehicle_ids = list(range(1, 101))  # Vehicle IDs from 1 to 100
    record_count = 0
    file_count = 1

    while True:
        for vehicle_id in vehicle_ids:
            vehicle_data = generate_vehicle_location(vehicle_id)

            # Print the data (optional)
            print(vehicle_data)

            # Write the data to the current JSON file
            write_to_json_file(vehicle_data, f'{output_directory}/vehicle_location_{file_count}.json')

            record_count += 1

            # Check if it's time to create a new JSON file
            if record_count >= records_per_file:
                record_count = 0
                file_count += 1

        time.sleep(interval_seconds)

if __name__ == "__main__":
    output_directory = 'J:\\Deployment\\Training & Testing\\Python\\Confluent\\output'
    continuously_stream_and_write_vehicle_data(output_directory)
