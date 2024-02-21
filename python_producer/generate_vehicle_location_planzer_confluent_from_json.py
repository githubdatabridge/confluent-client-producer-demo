from confluent_kafka import Producer
import time
import json
import os
import random

##########################################################################################################
# We need the vehicle_details_short and pln_res json files and this python code to be in the same folder #
##########################################################################################################


# Read configuration file for confluent cloud
def read_ccloud_config(config_file):
    omitted_fields = set(['schema.registry.url', 'basic.auth.credentials.source', 'basic.auth.user.info'])
    conf = {}
    with open(config_file) as fh:
        for line in fh:
            line = line.strip()
            if len(line) != 0 and line[0] != "#":
                parameter, value = line.strip().split('=', 1)
                if parameter not in omitted_fields:
                    conf[parameter] = value.strip()
    return conf


# Read the json file with dirver name and vehicle id
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


# Delivery report for message
def delivery_report(err, msg):
    if err is not None:
        print('Message delivery failed: {}'.format(err))
    else:
        print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))


def continuously_produce_vehicle_data(kafka_topic, vehicle_file_path, location_file_path):

    # Get connection configuration
    producer = Producer(read_ccloud_config("client.properties"))

    vehicle_data_list = read_vehicle_data_from_file(vehicle_file_path)
    location_data_list = read_vehicle_data_from_file(location_file_path)

    try:
        location_counter = 1
        while True:
            for location in location_data_list:
                    # print(f'Iteration: {location_counter}')
                    for vehicle in vehicle_data_list:
                        vehicle_id = vehicle['vehicle_id']
                        driver_name = vehicle['driver_name']
                        vehicle_location_data = generate_vehicle_location(location_counter,vehicle_id,driver_name,location_data_list)

                        # print(vehicle_location_data)
                        vehicle_data_json = json.dumps(vehicle_location_data)
                        # print(vehicle_data_json)
                        vehicle_id = vehicle_location_data["vehicle_id"]
                        # print(vehicle_id)

                        # Produce the data to Kafka topic
                        producer.produce(kafka_topic, key=str(vehicle_id), value=vehicle_data_json, callback=delivery_report)

                        # Flush messages to ensure they are sent to the broker
                        producer.flush()

                        # Sleep for a short period before producing more data
                        time.sleep(random.uniform(interval_seconds_internal_1,interval_seconds_internal_2))

                    location_counter += 1
                    time.sleep(interval_seconds)
    except KeyboardInterrupt:
        pass
    finally:
        # Flush any remaining messages before exiting
        producer.flush()


if __name__ == "__main__":
    # Interval of records generated in secods
    interval_seconds = 0.2
    interval_seconds_internal_1 = 0.2
    interval_seconds_internal_2 = 0.4

    kafka_topic = 'vehicle_position'

    current_directory = os.getcwd()
    vehicle_file_name = "vehicle_details_short.json"
    location_file_name = "pln_res.json"
    vehicle_file_path = os.path.join(current_directory, vehicle_file_name)
    location_file_path = os.path.join(current_directory, location_file_name)

    # print(file_path)
    continuously_produce_vehicle_data(kafka_topic, vehicle_file_path, location_file_path)
