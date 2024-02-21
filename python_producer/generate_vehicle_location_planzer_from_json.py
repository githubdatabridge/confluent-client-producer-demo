import time
import json
import os
import random

##########################################################################################################
# We need the vehicle_details_short and pln_res json files and this python code to be in the same folder #
##########################################################################################################

def read_vehicle_data_from_file(file_path):
    with open(file_path, 'r') as file:
        data = json.load(file)
    return data


def generate_vehicle_location(location_counter,vehicle_id,driver_name,location_data_list):
    
    vehicle_waiting = 0
    # print(vehicle_id)
    # print(driver_name)
    engine_temperature = round(random.uniform(95, 110), 2) # Randomized Engine temperature
    vehicle_speed = round(random.uniform(10, 40), 0) # Randomized Vehicle speed
    timestamp = int(time.time())

    # Start of location counter - All vehicles at start position
    if location_counter == 1:
        location_data = location_data_list[location_counter - 1]
        # location_key = location_data['key']
        latitude = location_data['location'][0]
        longitude = location_data['location'][1]
        # print(f'Vehicle ID: {vehicle_id} at start point {location_key} - {latitude} : {longitude}')

    # Vehicles still at starting point until their departure arrives
    if location_counter > 1 and vehicle_id > location_counter:
        location_data = location_data_list[0]
        # location_key = location_data['key']
        latitude = location_data['location'][0]
        longitude = location_data['location'][1]
        # print(f'Vehicle ID: {vehicle_id} still at start point {location_key} - {latitude} : {longitude}')
        vehicle_waiting = 1


    # Vehicles start moving
    if location_counter > 1 and vehicle_waiting != 1:
        # print(location_counter)
        starting_pos = 0
        starting_pos = vehicle_id - 1
        # print(f'starting_pos is {starting_pos}')

        location_mod = location_counter % 10
        location_data = location_data_list[location_mod - vehicle_id]
        # location_key = location_data['key']
        latitude = location_data['location'][0]
        longitude = location_data['location'][1]
        # print(f'Vehicle ID: {vehicle_id} currently at {location_key} - {latitude} : {longitude}')

    return {
        "vehicle_id": vehicle_id,
        "driver_name": driver_name,
        "location": {"latitude": latitude, "longitude": longitude},
        "engine_temperature": engine_temperature,
        "vehicle_speed" : vehicle_speed,
        "ts": timestamp
    }



def continuously_produce_vehicle_data(vehicle_file_path, location_file_path):
    vehicle_data_list = read_vehicle_data_from_file(vehicle_file_path)
    location_data_list = read_vehicle_data_from_file(location_file_path)

    location_counter = 1

    while True:
        for location in location_data_list:
                # print(f'Iteration: {location_counter}')
                for vehicle in vehicle_data_list:
                    vehicle_id = vehicle['vehicle_id']
                    driver_name = vehicle['driver_name']
                    print(generate_vehicle_location(location_counter,vehicle_id,driver_name,location_data_list))

                    time.sleep(random.uniform(interval_seconds_internal_1,interval_seconds_internal_2))

                location_counter += 1
                time.sleep(interval_seconds)



if __name__ == "__main__":
    # Interval of records generated in secods
    interval_seconds = 0.2
    interval_seconds_internal_1 = 0.2
    interval_seconds_internal_2 = 0.4

    current_directory = os.getcwd()
    vehicle_file_name = "vehicle_details_short.json"
    location_file_name = "pln_res.json"
    vehicle_file_path = os.path.join(current_directory, vehicle_file_name)
    location_file_path = os.path.join(current_directory, location_file_name)

    # print(file_path)
    continuously_produce_vehicle_data(vehicle_file_path, location_file_path)
