import psycopg2
from pymongo import MongoClient
from data_generator import psql_generate, mongo_data_generator



DB_NAME = "smart_building"


def connect_psql_db(dbname="postgres"):
    print(f"Connecting to {dbname}....")
    try:
        connection = psycopg2.connect(  user="postgres",
                                        password="Password",
                                        host="127.0.0.1",
                                        port="5432",
                                        database=dbname)
        connection.set_session(autocommit=True)
        print(f"Connected to {dbname}")
        return connection
    except (Exception, psycopg2.Error) as error:
        print("Error while connecting to PostgreSQL", error)


def create_psql_SB_db(conn):
    print("Creating Smart-Building database....")
    try:
        cur = conn.cursor()
        cur.execute("DROP DATABASE IF EXISTS " + DB_NAME)
        cur.execute("CREATE DATABASE " + DB_NAME)
        cur.close()
        print("Created Database " + DB_NAME)
    
    except (Exception, psycopg2.Error) as error:
        print("Error while creating Smart-Building database: ", error)


def create_psql_tables(conn):
    print("Creating tables buildings, floors, rooms, users, access_logs....")
    try:
        cur = conn.cursor()
        # Buildings Table
        cur.execute("""
            CREATE TABLE IF NOT EXISTS buildings (
                building_id SERIAL PRIMARY KEY,
                building_name VARCHAR(255),
                address VARCHAR(255),
                total_floors INT,
                construction_year INT,
                building_type VARCHAR(50),
                emergency_contact VARCHAR(50),
                maintenance_contact VARCHAR(50),
                energy_rating VARCHAR(2),
                building_status VARCHAR(50)
            );
        """)

        # Floors Table
        cur.execute("""
            CREATE TABLE floors (
                floor_id SERIAL PRIMARY KEY,
                building_id INT REFERENCES buildings(building_id),
                floor_number INT,
                description TEXT,
                total_rooms INT,
                floor_area FLOAT,
                fire_escape_plan TEXT,
                access_control_level VARCHAR(50)
            );
        """)

        # Rooms Table
        cur.execute("""
            CREATE TABLE IF NOT EXISTS rooms (
                room_id SERIAL PRIMARY KEY,
                floor_id INT REFERENCES floors(floor_id),
                room_name VARCHAR(255),
                room_type VARCHAR(50),
                room_size FLOAT,
                occupancy_limit INT,
                accessibility_features TEXT,
                room_status VARCHAR(50)
            );
        """)

        # Users Table
        cur.execute("""
            CREATE TABLE IF NOT EXISTS users (
                user_id SERIAL PRIMARY KEY,
                user_name VARCHAR(255),
                email VARCHAR(255),
                role VARCHAR(50),
                password_hash TEXT,
                date_joined DATE,
                last_login_date DATE,
                phone_number VARCHAR(50),
                emergency_contact VARCHAR(50),
                access_level VARCHAR(50)
            );
        """)

        # AccessLogs Table
        cur.execute("""
            CREATE TABLE IF NOT EXISTS access_logs (
                log_id SERIAL PRIMARY KEY,
                user_id INT REFERENCES users(user_id),
                room_id INT REFERENCES rooms(room_id),
                timestamp TIMESTAMP,
                access_type VARCHAR(50),
                access_method VARCHAR(50),
                access_status VARCHAR(50)
            );
        """)
        cur.close()
        print("Created tables")
    except (Exception, psycopg2.Error) as error:
        print("Error while creating tables: ", error)


def create_mongo_collections(db):

    print("Creating collections sensors, sensor_data, device_controls....")

    if db['sensors'] is not None:
        db['sensors'].drop()
    # Sensors Collection
    sensors = db['sensors']


    if db['sensor_data'] is not None:
        db['sensor_data'].drop()

    # SensorData Collection
    sensor_data = db['sensor_data']


    if db['device_controls'] is not None:
        db['device_controls'].drop()

    # DeviceControls Collection
    device_controls = db['device_controls']

    print("Created collections")


def compile_ids(conn):
    cur = conn.cursor()
    cur.execute("SELECT DISTINCT building_id FROM buildings")
    building_ids = cur.fetchall()
    building_ids = [building_id[0] for building_id in building_ids]

    cur.execute("SELECT DISTINCT floor_id FROM floors")
    floor_ids = cur.fetchall()
    floor_ids = [floor_id[0] for floor_id in floor_ids]

    cur.execute("SELECT DISTINCT room_id FROM rooms")
    room_ids = cur.fetchall()
    room_ids = [room_id[0] for room_id in room_ids]

    cur.execute("SELECT DISTINCT user_id FROM users")
    user_ids = cur.fetchall()
    user_ids = [user_id[0] for user_id in user_ids]

    return building_ids, floor_ids, room_ids, user_ids


def basic_data_retrival_psql(conn):

    cur = conn.cursor()

    # Count all Operational Buildings
    
    cur.execute(""" SELECT COUNT(*) 
                    FROM Buildings 
                    WHERE building_status = 'Operational';
                """)
    print("Number of Operational Buildings: ", cur.fetchall()[0][0])

    # Get all floors of building 1
    print("All floors of building 1: ")
    cur.execute("""
        SELECT * FROM floors f
        WHERE f.building_id = 1;
    """)
    for i,floor in enumerate(cur.fetchall()):
        print(f"{i}) {floor}")
    

    # Select office rooms with wheelchair accessibility and that is available for booking
    print("All Office rooms with wheelchair accessibility that is available for booking:")
    cur.execute("""
        SELECT * FROM rooms
        WHERE accessibility_features = 'Wheelchair accessible' and room_type = 'Office' and room_status = 'Available';
    """)
    for i,room in enumerate(cur.fetchall()):
        print(f"{i}) {room}")
    
    # Select users with admin role with name starting with 'A'
    print("All users with admin role: ")
    cur.execute("""
        SELECT * FROM users
        WHERE role = 'Admin' and user_name LIKE 'A%';
    """)
    for i,user in enumerate(cur.fetchall()):
        print(f"{i}) {user}")

    # Count the number of denied access logs
    cur.execute("""
        SELECT COUNT(*) FROM access_logs
        WHERE access_status = 'Denied';
    """)
    print("Number of denied access logs: ",cur.fetchall()[0][0])

    # Select access logs for a specific room
    print("Access logs for room (id=5): ")
    cur.execute("""
        SELECT * FROM access_logs
        WHERE room_id = 5;
    """)
    for i,log in enumerate(cur.fetchall()):
        print(f"{i}) {log}")

    cur.close()


def basic_data_retrival_mongo(db):

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


if __name__ == "__main__":
    # Connect to PostgreSQL
    conn = connect_psql_db()

    # Create Smart-Building database
    create_psql_SB_db(conn)

    # Connect to Smart-Building database
    conn = connect_psql_db(DB_NAME)

    # Create tables
    create_psql_tables(conn)

    # Connect to MongoDB
    client = MongoClient('mongodb://127.0.0.1:27017/')

    # Connect to Smart-Building database
    db = client[DB_NAME]

    # Create collections
    create_mongo_collections(db)


    # Generate and Insert data
    psql_generate(conn)

    # Compile ids
    building_ids, floor_ids, room_ids, user_ids = compile_ids(conn)

    # Generate and Insert data
    mongo_data_generator(db, room_ids)

    # Basic Data Retrieval Queries

    basic_data_retrival_psql(conn)
    basic_data_retrival_mongo(db)
    





    # Close the connection
    client.close()
    conn.close()






