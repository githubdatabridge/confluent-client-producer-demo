import random
import time
import json
import os

#######################################################################################
# We need the vehicle_details.json file and this python code to be in the same folder #
#######################################################################################


def read_vehicle_data_from_file(file_path):
    with open(file_path, 'r') as file:
        data = json.load(file)
    return data


def generate_vehicle_location(vehicle_data):
    latitude = round(random.uniform(47.3645, 47.4155), 6)           # Latitude for Zurich
    longitude = round(random.uniform(8.5004, 8.5687), 6)        # Longitude for Zurich
    timestamp = int(time.time())
    
    vehicle_id = vehicle_data["vehicle_id"]
    driver_name = vehicle_data["driver_name"]
    engine_temperature = round(random.uniform(95, 110), 2) # Randomized Engine temperature
    vehicle_speed = round(random.uniform(0, 20), 0) # Randomized Vehicle speed

    return {
        "vehicle_id": vehicle_id,
        "driver_name": driver_name,
        "location": {"latitude": latitude, "longitude": longitude},
        "engine_temperature": engine_temperature,
        "vehicle_speed" : vehicle_speed,
        "ts": timestamp
    }


def continuously_produce_vehicle_data(vehicle_data_file_path):
    vehicle_data_list = read_vehicle_data_from_file(vehicle_data_file_path)

    while True:
        for vehicle_data in vehicle_data_list:
            vehicle_location_data = generate_vehicle_location(vehicle_data)
            print(vehicle_location_data)
            # print(vehicle_location_data["vehicle_id"])

            # Sleep for a short period before producing more data
            time.sleep(interval_seconds)


if __name__ == "__main__":
    # Interval of records generated in secods
    interval_seconds = 0.01

    current_directory = os.getcwd()
    # print(current_directory)
    file_name = "vehicle_details.json"
    file_path = os.path.join(current_directory, file_name)
    # print(file_path)
    continuously_produce_vehicle_data(file_path)
