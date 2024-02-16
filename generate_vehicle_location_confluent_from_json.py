from confluent_kafka import Producer
import os
import random
import time
import json


#######################################################################################
# We need the vehicle_details.json file and this python code to be in the same folder #
#######################################################################################


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


# Generate random vehicle location
def generate_vehicle_location(vehicle_data):
    latitude = round(random.uniform(47.3645, 47.4155), 6)           # Latitude for Zurich
    longitude = round(random.uniform(8.5004, 8.5687), 6)        # Longitude for Zurich
    timestamp = int(time.time())
    
    vehicle_id = vehicle_data["vehicle_id"]
    driver_name = vehicle_data["driver_name"]
    engine_temperature = round(random.uniform(95, 110), 2)          # Randomized Engine temperature
    vehicle_speed = round(random.uniform(0, 20), 0)                 # Randomized Vehicle speed

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


# Producing coninuous data
def continuously_produce_vehicle_data(topic, vehicle_data_file_path, interval):
    vehicle_data_list = read_vehicle_data_from_file(vehicle_data_file_path)
    # print(vehicle_data_list)
    # vehicle_ids = list(range(1, 101))  # Vehicle IDs from 1 to 100

    # Get connection configuration
    producer = Producer(read_ccloud_config("client.properties"))

    try:
        while True:
            for vehicle_data in vehicle_data_list:
                vehicle_location_data = generate_vehicle_location(vehicle_data)
                # print(vehicle_location_data)
                vehicle_data_json = json.dumps(vehicle_location_data)
                # print(vehicle_data_json)
                vehicle_id = vehicle_location_data["vehicle_id"]
                # print(vehicle_id)

                # Produce the data to Kafka topic
                producer.produce(topic, key=str(vehicle_id), value=vehicle_data_json, callback=delivery_report)

                # Flush messages to ensure they are sent to the broker
                producer.flush()

                # Sleep for a short period before producing more data
                time.sleep(interval)

    except KeyboardInterrupt:
        pass
    finally:
        # Flush any remaining messages before exiting
        producer.flush()


# Main
if __name__ == "__main__":
    kafka_topic = 'vehicle_position'                                            # Kafka topic
    current_directory = os.getcwd()                                             # Get current folder location
    file_name = "vehicle_details.json"                                          # Location of json file with driver name and vehicle id
    file_path = os.path.join(current_directory, file_name)
    interval = 0.1                                                              # Interval to produce records
    continuously_produce_vehicle_data(kafka_topic, file_path, interval)
