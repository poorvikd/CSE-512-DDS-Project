# 1. Horizontal Fragmentation: Split tables into subsets based on specific criteria
# 2. Vertical Fragmentation: Divide tables into smaller subsets based on columns to optimize data retrieval.
# 3. Replication Setup: Configure replication models such as master-slave or peer-to-peer replication to enhance data availability.

import psutil
import psycopg2
from pymongo import MongoClient
import time
from pprint import pprint

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

def create_vertical_partitions(conn):
    # Vertical Fragmentation on Rooms Table
    # Criteria: Frequency of column access.
    # Implementation: Split the Rooms table into two: one part containing frequently accessed columns like RoomID, 
    # RoomName, RoomType, and another part with less frequently used columns like RoomSize, OccupancyLimit, AccessibilityFeatures.

    print("Vertical Fragmentation on Rooms Table")
    print("Criteria: Frequency of column access")
    print("Implementation: Split the Rooms table into two: one part containing frequently accessed columns like RoomID, RoomName, RoomType, and another part with less frequently used columns like RoomSize, OccupancyLimit, AccessibilityFeatures.")

    try:
        cur = conn.cursor()

        # Create the frequently accessed table
        cur.execute("""
            CREATE TABLE room_frequently_accessed_partition (
                room_id SERIAL PRIMARY KEY,
                room_name VARCHAR(255),
                room_type VARCHAR(50),
                floor_id INT REFERENCES floors(floor_id),
                room_status VARCHAR(50)
            );
        """)

        # Copy data from the original Rooms table to the frequently accessed table
        cur.execute("""
            INSERT INTO room_frequently_accessed_partition (room_id, room_name, room_type, floor_id, room_status)
            SELECT room_id, room_name, room_type, floor_id, room_status
            FROM rooms;
        """)

        # Create the less frequently accessed table
        cur.execute("""
            CREATE TABLE room_less_frequently_accessed_partition (
                room_id SERIAL PRIMARY KEY,
                room_size FLOAT,
                occupancy_limit INT,
                accessibility_features TEXT
            );
        """)

        # Copy data from the original Rooms table to the less frequently accessed table
        cur.execute("""
            INSERT INTO room_less_frequently_accessed_partition (room_id, room_size, occupancy_limit, accessibility_features)
            SELECT room_id, room_size, occupancy_limit, accessibility_features
            FROM Rooms;
        """)

        print("Displaying 10 records from room_frequently_accessed_partition")
        cur.execute("""
            SELECT * FROM room_frequently_accessed_partition LIMIT 10;
        """)

        for i,row in enumerate(cur.fetchall()):
            print(f"{i}. {row}")
        
        print("Displaying 10 records from room_less_frequently_accessed_partition")
        cur.execute("""
            SELECT * FROM room_less_frequently_accessed_partition LIMIT 10;
        """)

        for i,row in enumerate(cur.fetchall()):
            print(f"{i}. {row}")

        cur.close()

        print("Vertical Fragmentation on Rooms Table complete")
    
    except (Exception, psycopg2.Error) as error:
        print("Error while performing Vertical Fragmentation on Rooms Table: ", error)

    return


def create_horizontal_partitions(conn):
    # Horizontal Fragmentation on Users Table
    # Criteria: Role of the user.
    # Implementation: Split the Users table into three parts based on the role of the user: Admin, Manager, Employee.
    print("Creating List partitions on Users Table")
    print("Criteria: Role of the user")
    print("Implementation: Split the Users table into three parts based on the role of the user: Admin, Manager, Employee.")

    try:
        cur = conn.cursor()

        # Create a copy of the Users table with partition on role

        cur.execute("""
            CREATE TABLE IF NOT EXISTS users_copy (
                user_id INT,
                user_name VARCHAR(255),
                email VARCHAR(255),
                role VARCHAR(50),
                password_hash TEXT,
                date_joined DATE,
                last_login_date DATE,
                phone_number VARCHAR(50),
                emergency_contact VARCHAR(255),
                access_level VARCHAR(50)
            ) PARTITION BY LIST (role);
        """)

        # Create a partition for Admin users
        cur.execute("""
            CREATE TABLE IF NOT EXISTS users_admin_partition
            PARTITION OF users_copy
            FOR VALUES IN ('Admin');
        """)

        # Create a partition for Manager users
        cur.execute("""
            CREATE TABLE IF NOT EXISTS users_manager_partition
            PARTITION OF users_copy
            FOR VALUES IN ('Manager');
        """)

        # Create a partition for Employee users
        cur.execute("""
            CREATE TABLE IF NOT EXISTS users_employee_partition
            PARTITION OF users_copy
            FOR VALUES IN ('Employee');
        """)

        # Insert data into the partitions from Users table
        cur.execute("""
            INSERT INTO users_copy
            SELECT * FROM users;
        """)

        print("List partitions on Users Table based on their roles complete")

        print("Displaying 10 records with role Admin from users_admin_partition")
        cur.execute("""
            SELECT * FROM users_admin_partition LIMIT 10;
        """)

        for i,row in enumerate(cur.fetchall()):
            print(f"{i}. {row}")
        
        print("Displaying 10 records with role Manager from users_manager_partition")

        cur.execute("""
            SELECT * FROM users_manager_partition LIMIT 10;
        """)

        for i,row in enumerate(cur.fetchall()):
            print(f"{i}. {row}")

        print("Displaying 10 records with role Staff from users_employee_partition")

        cur.execute("""
            SELECT * FROM users_employee_partition LIMIT 10;
        """)

        for i,row in enumerate(cur.fetchall()):
            print(f"{i}. {row}")
    
        cur.close()
    
    except (Exception, psycopg2.Error) as error:
        print("Error while performing List partitions on Users Table based on their roles: ", error)


    # Horizontal Fragmentation on AccessLogs Table
    # Criteria: Timestamp of the access.
    # Implementation: Split the AccessLogs table into three parts based on the timestamp of the access: 2021, 2022, 2023.
    print("Creating Range partitions on AccessLogs Table")
    print("Criteria: Timestamp of the access")
    print("Implementation: Split the AccessLogs table into three parts based on the timestamp of the access: 2021, 2022, 2023.")
    
    try:
        cur = conn.cursor()

        # Create a copy of the AccessLogs table with partition on timestamp

        cur.execute("""
            CREATE TABLE IF NOT EXISTS access_logs_copy (
                log_id INT,
                user_id INT REFERENCES users(user_id),
                room_id INT REFERENCES rooms(room_id),
                timestamp TIMESTAMP,
                access_type VARCHAR(50),
                access_method VARCHAR(50),
                access_status VARCHAR(50)
            ) PARTITION BY RANGE (timestamp);
        """)

        # Create a partition for 2021
        cur.execute("""
            CREATE TABLE IF NOT EXISTS access_logs_2021_partition
            PARTITION OF access_logs_copy
            FOR VALUES FROM ('2021-01-01') TO ('2021-12-31');
        """)

        # Create a partition for 2022
        cur.execute("""
            CREATE TABLE IF NOT EXISTS access_logs_2022_partition
            PARTITION OF access_logs_copy
            FOR VALUES FROM ('2022-01-01') TO ('2022-12-31');
        """)

        # Create a partition for 2023
        cur.execute("""
            CREATE TABLE IF NOT EXISTS access_logs_2023_partition
            PARTITION OF access_logs_copy
            FOR VALUES FROM ('2023-01-01') TO ('2023-12-31');
        """)

        # Insert data into the partitions from AccessLogs table

        cur.execute("""
            INSERT INTO access_logs_copy
            SELECT * FROM access_logs;
        """)

        print("Range partitions on AccessLogs Table based on timestamp complete")

        print("Displaying 10 records from access_logs_2021_partition")
        cur.execute("""
            SELECT * FROM access_logs_2021_partition LIMIT 10;
        """)

        for i,row in enumerate(cur.fetchall()):
            print(f"{i}. {row}")

        print("Displaying 10 records from access_logs_2022_partition")

        cur.execute("""
            SELECT * FROM access_logs_2022_partition LIMIT 10;
        """)

        for i,row in enumerate(cur.fetchall()):
            print(f"{i}. {row}")

        print("Displaying 10 records from access_logs_2023_partition")

        cur.execute("""
            SELECT * FROM access_logs_2023_partition LIMIT 10;
        """)

        for i,row in enumerate(cur.fetchall()):
            print(f"{i}. {row}")

        cur.close()

    except (Exception, psycopg2.Error) as error:
        print("Error while performing Range partitions on AccessLogs Table based on timestamp: ", error)
    
    return

def find_process_by_port(port):
    """
    Find a process by the port it is listening on.
    """
    for process in psutil.process_iter(['pid', 'name', 'connections']):
        connections = process.info.get('connections')
        if connections:
            for conn in connections:
                if conn.laddr and conn.laddr.port == port:
                    return process.info['pid']
    return None

def kill_process_by_port(port):
    """
    Kill a process by the port it is listening on.
    """
    pid = find_process_by_port(port)
    if pid is not None:
        try:
            process = psutil.Process(pid)
            process.terminate()  # or process.kill() for forceful termination
            print(f"Process with PID {pid} on port {port} terminated.")
            
        except psutil.NoSuchProcess as e:
            print(f"Error: {e}")
    else:
        print(f"No process found on port {port}.")


def replication_interaction():
    # Interactions with replica set using pymongo
    # Connect to MongoDB
    print("Connecting to MongoDB....")
    client = MongoClient('mongodb://localhost:27017,localhost:27018,localhost:27019/?replicaSet=SmartBuilding')
    print("Connected to MongoDB")

    # Status of the replica set
    print("Status of the replica set")
    pprint(client.admin.command('replSetGetStatus')['members'])

    # Insert a document into the collection
    print("Inserting a document into the collection. (All writes are sent to primary)")
    db = client['smart_building']
    sensors = db['sensors']
    sensor_data = db['sensor_data']
    device_controls = db['device_controls']

    sensor_data.insert_one({
        "sensor_id": 1,
        "sensor_type": "Temperature",
        "sensor_value": 25,
        "timestamp": "2021-04-01 12:00:00"
    })

    # Read the document from the collection
    print("Reading the document from the collection")
    pprint(sensor_data.find_one())

    print("Failover Scenario")

    # Kill the primary node
    print("Killing the primary node")

    kill_process_by_port(27017)
    time.sleep(10)

    # Status of the replica set
    print("Status of the replica set")
    pprint(client.admin.command('replSetGetStatus')['members'])

    # Reading the document
    print("Reading the document from the collection")
    pprint(sensor_data.find_one())
    



    


if __name__ == '__main__':
    DB_NAME = 'smart_building'
    # Connect to PostgreSQL
    conn = connect_psql_db(DB_NAME)

    # # Create Vertical Partitions
    create_vertical_partitions(conn)

    # # Create Horizontal Partitions
    create_horizontal_partitions(conn)

    # Setup Replication
    replication_interaction()



