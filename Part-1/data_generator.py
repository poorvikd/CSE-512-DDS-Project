import psycopg2
from faker import Faker
import random
from datetime import date, timedelta


fake = Faker()

# Function to generate a random date within a given range
def random_date(start_date, end_date):
    return start_date + timedelta(
        days=random.randint(0, (end_date - start_date).days)
    )


def psql_generate(conn):
    cur = conn.cursor()
    # Generate data for Buildings table
    for _ in range(100):
        building_name = fake.company()
        address = fake.address().replace('\n', ', ')
        total_floors = random.randint(1, 10)
        construction_year = random.randint(1960, 2023)
        building_type = random.choice(['Office Building', 'Residential Building', 'Commercial Building'])
        emergency_contact = fake.phone_number()
        maintenance_contact = fake.phone_number()
        energy_rating = random.choice(['A', 'B', 'C', 'D'])
        building_status = random.choice(['Operational', 'Under Construction'])

        cur.execute("""
            INSERT INTO buildings (
                building_name, address, total_floors, construction_year,
                building_type, emergency_contact, maintenance_contact,
                energy_rating, building_status
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING building_id;
        """, (building_name, address, total_floors, construction_year, building_type,
            emergency_contact, maintenance_contact, energy_rating, building_status))
        building_id = cur.fetchone()[0]

        # Generate data for Floors table
        for floor_number in range(1, total_floors + 1):
            description = f'Floor {floor_number}'
            total_rooms = random.randint(10, 20)
            floor_area = random.uniform(500.0, 2000.0)
            fire_escape_plan = fake.text()
            access_control_level = random.choice(['High', 'Medium', 'Low'])

            cur.execute("""
                INSERT INTO floors (
                    building_id, floor_number, description, total_rooms,
                    floor_area, fire_escape_plan, access_control_level
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                RETURNING floor_id;
            """, (building_id, floor_number, description, total_rooms,
                floor_area, fire_escape_plan, access_control_level))
            floor_id = cur.fetchone()[0]

            # Generate data for Rooms table
            for _ in range(total_rooms):
                room_name = f'Room {random.randint(1, 100)}'
                room_type = random.choice(['Office', 'Conference', 'Utility'])
                room_size = random.uniform(20.0, 200.0)
                occupancy_limit = random.randint(1, 10)
                accessibility_features = random.choice(['Wheelchair accessible', 'Not accessible'])
                room_status = random.choice(['Available', 'Occupied', 'Under Maintenance'])

                cur.execute("""
                    INSERT INTO rooms (
                        floor_id, room_name, room_type, room_size,
                        occupancy_limit, accessibility_features, room_status
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s);
                """, (floor_id, room_name, room_type, room_size,
                    occupancy_limit, accessibility_features, room_status))
    
    # Generate data for Users table
    for _ in range(100):
        user_name = fake.name()
        email = fake.email()
        role = random.choice(['Admin', 'Manager', 'Employee'])
        password_hash = fake.password()
        date_joined = random_date(date(2015, 1, 1), date(2021, 1, 1))
        last_login_date = random_date(date(2021, 1, 1), date.today())
        phone_number = fake.phone_number()
        emergency_contact = fake.phone_number()
        access_level = random.choice(['High', 'Medium', 'Low'])

        cur.execute("""
            INSERT INTO users (
                user_name, email, role, password_hash,
                date_joined, last_login_date, phone_number,
                emergency_contact, access_level
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);
        """, (user_name, email, role, password_hash,
            date_joined, last_login_date, phone_number,
            emergency_contact, access_level))
    
    # Generate data for AccessLogs table
    for _ in range(100):
        user_id = random.randint(1, 100)
        room_id = random.randint(1, 100)
        timestamp = random_date(date(2021, 1, 1), date.today())
        access_type = random.choice(['Entry', 'Exit'])
        access_method = random.choice(['Card', 'Key', 'Fingerprint'])
        access_status = random.choice(['Granted', 'Denied'])

        cur.execute("""
            INSERT INTO access_logs (
                user_id, room_id, timestamp, access_type,
                access_method, access_status
            )
            VALUES (%s, %s, %s, %s, %s, %s);
        """, (user_id, room_id, timestamp, access_type,
            access_method, access_status))
    
    cur.close()
    return 


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


def generate_sensor_data(room_id):
    sensor_type = random.choice(list(SENSOR.keys()))
    return {
        'sensor_id': fake.random_int(1, 1000),
        'room_id':  random.choice(room_id),
        'sensor_type': sensor_type,
        'model': random.choice(SENSOR[sensor_type]['models']),
        'manufacturer': random.choice(SENSOR[sensor_type]['manufacturer']),
        'installation_date': fake.date_this_decade().isoformat(),
        'last_maintenance_date': fake.date_this_year().isoformat(),
        'sensor_status': random.choice(['active', 'inactive']),
        'firmware_version': random.choice(SENSOR[sensor_type]['firmware_version']),
        'communication_protocol': random.choice(SENSOR[sensor_type]['communication_protocol'])
    }

# Generate data for SensorData collection
def generate_sensor_data_data(sensor_id, sensor_type):
    return {
        'data_id': fake.random_int(1, 1000),
        'sensor_id': sensor_id,
        'timestamp': fake.date_time_this_decade().isoformat(),
        'data_type': sensor_type,
        'data_value': random.choice(SENSOR[sensor_type]['values']),
        'unit_of_measure': SENSOR[sensor_type]['unit'],
        'data_quality': random.choice(['good', 'bad']),
        'data_status': random.choice(['confirmed', 'unconfirmed'])
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

# Generate data for DeviceControls collection
def generate_device_controls_data(room_id):
    device_type = random.choice(list(DEVICE_CONTROL.keys()))
    return {
        'device_id': fake.random_int(1, 100),
        'room_id': random.choice(room_id),
        'device_type': device_type,
        'last_updated': fake.date_time_this_decade().isoformat(),
        'control_protocol': random.choice(DEVICE_CONTROL[device_type]['control_protocol']),
        'firmware_version': random.choice(DEVICE_CONTROL[device_type]['firmware_version']),
        'power_consumption': random.choice(DEVICE_CONTROL[device_type]['power_consumption']),
        'device_status': random.choice(DEVICE_CONTROL[device_type]['device_status'])
    }


def mongo_data_generator(db, room_id):
    # Insert data into collections
    for _ in range(200):
        sensor_data = generate_sensor_data(room_id)
        db['sensors'].insert_one(sensor_data)

        sensor_data_data = generate_sensor_data_data(sensor_data['sensor_id'], sensor_data['sensor_type'])
        db['sensor_data'].insert_one(sensor_data_data, room_id)

        device_controls_data = generate_device_controls_data(room_id)
        db['device_controls'].insert_one(device_controls_data)

    return