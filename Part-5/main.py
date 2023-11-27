# To-Do Tasks
# 1. Choose an appropriate NoSQL database system based on your
# project's topic and data requirements. Define and document the
# data schema and data model tailored to your topic's data storage
# needs.
# 2. Implement basic CRUD (Create, Read, Update, Delete)
# operations for the domain-specific data.
# 3. Create sample queries and data retrieval operations to
# showcase the functionality of your NoSQL database for your
# chosen topic.

from pymongo import MongoClient
from faker import Faker
import random
from pprint import pprint



def create(db):
    print("Inserting data into Sensor, Sensor Data, and Device Control collections")
    sensor_type = random.choice(list(SENSOR.keys()))
    sensor_data = {
        'sensor_id': 1001,
        'room_id':  500,
        'sensor_type': sensor_type,
        'model': random.choice(SENSOR[sensor_type]['models']),
        'manufacturer': random.choice(SENSOR[sensor_type]['manufacturer']),
        'installation_date': fake.date_this_decade().isoformat(),
        'last_maintenance_date': fake.date_this_year().isoformat(),
        'sensor_status': random.choice(['active', 'inactive']),
        'firmware_version': random.choice(SENSOR[sensor_type]['firmware_version']),
        'communication_protocol': random.choice(SENSOR[sensor_type]['communication_protocol'])
    }
    db['sensors'].insert_one(sensor_data)

    pprint(sensor_data)

    sensor_data_data = {
        'data_id': 1001,
        'sensor_id': sensor_data['sensor_id'],
        'timestamp': fake.date_time_this_decade().isoformat(),
        'data_type': sensor_type,
        'data_value': random.choice(SENSOR[sensor_type]['values']),
        'unit_of_measure': SENSOR[sensor_type]['unit'],
        'data_quality': random.choice(['good', 'bad']),
        'data_status': random.choice(['confirmed', 'unconfirmed'])
    }
    db['sensor_data'].insert_one(sensor_data_data)

    pprint(sensor_data_data)

    device_type = random.choice(list(DEVICE_CONTROL.keys()))
    device_type_data = {
        'device_id': 101,
        'room_id': sensor_data['room_id'],
        'device_type': device_type,
        'last_updated': fake.date_time_this_decade().isoformat(),
        'control_protocol': random.choice(DEVICE_CONTROL[device_type]['control_protocol']),
        'firmware_version': random.choice(DEVICE_CONTROL[device_type]['firmware_version']),
        'power_consumption': random.choice(DEVICE_CONTROL[device_type]['power_consumption']),
        'device_status': random.choice(DEVICE_CONTROL[device_type]['device_status'])
    }
    db['device_controls'].insert_one(device_type_data)

    pprint(device_type_data)

    print("Data inserted successfully")

def read(db):
    print("Reading data from Sensor, Sensor Data, and Device Control collections that were inserted")
    print("Sensor: ")
    for i,data in enumerate(db['sensors'].find({'sensor_id': 1001,'room_id': 500})):
        pprint(data)
    print()
    print("Sensor Data: ")
    for i,data in enumerate(db['sensor_data'].find({'data_id':1001,'sensor_id': 1001})):
        pprint(data)
    print()
    print("Device Control: ")
    for i,data in enumerate(db['device_controls'].find({'device_id':101,'room_id': 500})):
        pprint(data)

def update(db):
    print("Updating data in Sensor, Sensor Data, and Device Control collections that were inserted")
    print("Updating sensor status to inactive, sensor data quality to bad, and device status to not functioning")
    db['sensors'].update_one({'sensor_id': 1001}, {'$set': {'sensor_status': 'inactive'}})
    db['sensor_data'].update_one({'data_id': 1001}, {'$set': {'data_quality': 'bad'}})
    db['device_controls'].update_one({'device_id': 101}, {'$set': {'device_status': 'not functioning'}})
    print("Data updated successfully")

def delete(db):
    print("Deleting data in Sensor, Sensor Data, and Device Control collections that were inserted")
    db['sensors'].delete_one({'sensor_id': 1001,'room_id': 500})
    db['sensor_data'].delete_one({'data_id':1001,'sensor_id': 1001})
    db['device_controls'].delete_one({'device_id':101,'room_id': 500})
    print("Data deleted successfully")


def run_queries(db):
    # Find all sensors with sensor type 'Temperature' and model 'T3000
    print("All sensors with sensor type temperature and model T3000: ")
    sensors = db['sensors']
    for i,sensor in enumerate(sensors.find({'sensor_type': 'temperature', 'model': 'T3000'})):
        print(f"{i}. {sensor}")

    # Count all inactive sensors
    print("Number of inactive sensors: ", sensors.count_documents({'sensor_status': 'inactive'}))

    # Find Sensor Data with id = 158
    print("Sensor Data with id = 158: ")
    sensor_data = db['sensor_data']
    for i,data in enumerate(sensor_data.find({'sensor_id': 158})):
        print(f"{i}. {data}")
    

    # Count sensor data with poor quality
    print("Number of sensor data with bad quality: ", sensor_data.count_documents({'data_quality': 'bad'}))


    # Find all device controls with device type 'CCTV' and running the ZigBee communication protocol
    print("All device controls with device type 'CCTV' and running the ZigBee communication protocol: ")
    device_controls = db['device_controls']
    for i,control in enumerate(device_controls.find({'device_type':'CCTV','control_protocol':'Zigbee'})):
        print(f"{i}. {control}")



if __name__ == '__main__':

    fake = Faker()

    SENSOR = {
        "temperature": {
            "models": ["T1000", "T2000", "T3000"],
            "manufacturer": ["SensorCorp","SensorTech","AllSensor"],
            "communication_protocol": ["WiFi", "Bluetooth", "Zigbee"],
            "firmware_version": ["v1.2.3", "v2.3.4", "v3.4.5"],
            "unit": "farhenheit",
            "values" : [fake.random.uniform(50, 90) for _ in range(100)],
        },
        "humidity": {
            "models": ["H1000", "H2000", "H3000"],
            "manufacturer": ["SensorCorp","SensorTech","AllSensor"],
            "communication_protocol": ["WiFi", "Bluetooth", "Zigbee"],
            "firmware_version": ["v1.2.3", "v2.3.4", "v3.4.5"],
            "unit": "percentage",
            "values" : [fake.random.uniform(30, 60) for _ in range(100)],
        },
        "light": {
            "models": ["L1000", "L2000", "L3000"],
            "manufacturer": ["SensorCorp","SensorTech","AllSensor"],
            "communication_protocol": ["WiFi", "Bluetooth", "Zigbee"],
            "firmware_version": ["v1.2.3", "v2.3.4", "v3.4.5"],
            "unit": "lux",
            "values" : [fake.random.uniform(200, 1000) for _ in range(100)],
        },
        "motion": {
            "models": ["M1000", "M2000", "M3000"],
            "manufacturer": ["SensorCorp","SensorTech","AllSensor"],
            "communication_protocol": ["WiFi", "Bluetooth", "Zigbee"],
            "firmware_version": ["v1.2.3", "v2.3.4", "v3.4.5"],
            "unit": "boolean",
            "values" : ["True", "False"],
        }
    }

    DEVICE_CONTROL = {
        "Thermostat":{
            "control_protocol": ["WiFi", "Bluetooth", "Zigbee"],
            "firmware_version": ["v1.2.3", "v2.3.4", "v3.4.5"],
            "power_consumption": [fake.random.uniform(1, 10) for _ in range(100)],
            "device_status": ["functioning", "not functioning"],
        },
        "Lighting":{
            "control_protocol": ["WiFi", "Bluetooth", "Zigbee"],
            "firmware_version": ["v1.2.3", "v2.3.4", "v3.4.5"],
            "power_consumption": [fake.random.uniform(1, 10) for _ in range(100)],
            "device_status": ["functioning", "not functioning"],
        },
        "AirConditioner":{
            "control_protocol": ["WiFi", "Bluetooth", "Zigbee"],
            "firmware_version": ["v1.2.3", "v2.3.4", "v3.4.5"],
            "power_consumption": [fake.random.uniform(1, 10) for _ in range(100)],
            "device_status": ["functioning", "not functioning"],
        },
        "CCTV":{
            "control_protocol": ["WiFi", "Bluetooth", "Zigbee"],
            "firmware_version": ["v1.2.3", "v2.3.4", "v3.4.5"],
            "power_consumption": [fake.random.uniform(1, 10) for _ in range(100)],
            "device_status": ["functioning", "not functioning"],
        },
    }

    DB_NAME = "smart_building"
    
    # Connect to MongoDB
    client = MongoClient('mongodb://127.0.0.1:27017/')

    # Connect to Smart-Building database
    db = client[DB_NAME]

    # CRUD Operations

    create(db)
    read(db)
    update(db)
    read(db)
    delete(db)
    read(db)
    
    run_queries(db)

    client.close()