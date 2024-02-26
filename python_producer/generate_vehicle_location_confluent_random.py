from confluent_kafka import Producer
from faker import Faker
import random
import time
import json

fake = Faker()

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


def generate_vehicle_location(vehicle_id):
    latitude = round(random.uniform(47.3645, 47.4155), 6)           # Latitude for Zurich
    longitude = round(random.uniform(8.5004, 8.5687), 6)        # Longitude for Zurich
    timestamp = int(time.time())
    engine_temperature = round(random.uniform(95, 110), 2)
    driver_name = fake.name()
    
    return {
        "vehicle_id": vehicle_id,
        "location": {"latitude": latitude, "longitude": longitude},
        "ts": timestamp,
        "driver_name" : driver_name,
        "engine_temperature" : engine_temperature
    }

def delivery_report(err, msg):
    if err is not None:
        print('Message delivery failed: {}'.format(err))
    else:
        print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))

def continuously_produce_vehicle_data(topic):
    vehicle_ids = list(range(1, 101))  # Vehicle IDs from 1 to 100

    # Get connection configuration
    producer = Producer(read_ccloud_config("client.properties"))

    try:
        while True:
            for vehicle_id in vehicle_ids:
                vehicle_data = generate_vehicle_location(vehicle_id)
                vehicle_data_json = json.dumps(vehicle_data)

                # Produce the data to Kafka topic
                producer.produce(topic, key=str(vehicle_id), value=vehicle_data_json, callback=delivery_report)

                # Flush messages to ensure they are sent to the broker
                producer.flush()

                # Sleep for a short period before producing more data
                time.sleep(1)

    except KeyboardInterrupt:
        pass
    finally:
        # Flush any remaining messages before exiting
        producer.flush()

if __name__ == "__main__":
    # Add kafka topic name
    kafka_topic = 'vehicle_position'

    continuously_produce_vehicle_data(kafka_topic)
