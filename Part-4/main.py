import psycopg2 
import time
from datetime import date, timedelta
from multiprocessing import Process


DB_NAME = "smart_building"

def connect_pgpool_db(dbname="postgres"):
    print(f"Connecting to {dbname}....")
    try:
        connection = psycopg2.connect(  user="postgres",
                                        host="localhost",
                                        port="9999",
                                        database=dbname)
        print(f"Connected to {dbname}")
        return connection
    except (Exception, psycopg2.Error) as error:
        print("Error while connecting to PostgreSQL", error)

def connect_psql_db(dbname="postgres"):
    print(f"Connecting to {dbname}....")
    try:
        connection = psycopg2.connect(  user="postgres",
                                        password="Password",
                                        host="localhost",
                                        port="5432",
                                        database=dbname)
        print(f"Connected to {dbname}")
        return connection
    except (Exception, psycopg2.Error) as error:
        print("Error while connecting to PostgreSQL", error)

def create_pgpool_SB_db(conn):
    print("Creating Smart-Building database in pgpool....")
    try:
        cursor = conn.cursor()
        cursor.execute("DROP DATABASE IF EXISTS " + DB_NAME)
        cursor.execute("CREATE DATABASE " + DB_NAME)
        conn.commit()
        cursor.close()
        print("Created Database " + DB_NAME)
    
    except (Exception, psycopg2.Error) as error:
        print("Error while creating Smart-Building database: ", error)

def create_psql_tables(conn):
    print("Creating tables buildings, floors, rooms, users, access_logs in pgpool....")
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
                emergency_contact VARCHAR(15),
                maintenance_contact VARCHAR(15),
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
        conn.commit()
        cur.close()
        print("Created tables")
    except (Exception, psycopg2.Error) as error:
        print("Error while creating tables: ", error)

def insert_psql_data(conn,conn_psql):
    # Inserting data into pgpool from psql
    print("Inserting data into pgpool from psql")
    try:
        cur = conn.cursor()
        cur_psql = conn_psql.cursor()
        cur_psql.execute("SELECT * FROM buildings;")
        buildings = cur_psql.fetchall()
        for building in buildings:
            cur.execute("""
                INSERT INTO buildings (
                    building_name, address, total_floors,
                    construction_year, building_type, emergency_contact,
                    maintenance_contact, energy_rating, building_status
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);
            """, (building[1], building[2], building[3], building[4],
                building[5], building[6], building[7], building[8], building[9]))
        
        cur_psql.execute("SELECT * FROM floors;")
        floors = cur_psql.fetchall()
        for floor in floors:
            cur.execute("""
                INSERT INTO floors (
                    building_id, floor_number, description, total_rooms,
                    floor_area, fire_escape_plan, access_control_level
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s);
            """, (floor[1], floor[2], floor[3], floor[4],
                floor[5], floor[6], floor[7]))
        
        cur_psql.execute("SELECT * FROM rooms;")
        rooms = cur_psql.fetchall()
        for room in rooms:
            cur.execute("""
                INSERT INTO rooms (
                    floor_id, room_name, room_type, room_size,
                    occupancy_limit, accessibility_features, room_status
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s);
            """, (room[1], room[2], room[3], room[4],
                room[5], room[6], room[7]))
        
        cur_psql.execute("SELECT * FROM users;")
        users = cur_psql.fetchall()
        for user in users:
            cur.execute("""
                INSERT INTO users (
                    user_name, email, role, password_hash,
                    date_joined, last_login_date, phone_number,
                    emergency_contact, access_level
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);
            """, (user[1], user[2], user[3], user[4],
                user[5], user[6], user[7], user[8], user[9]))
            
        cur_psql.execute("SELECT * FROM access_logs;")
        access_logs = cur_psql.fetchall()
        for access_log in access_logs:
            cur.execute("""
                INSERT INTO access_logs (
                    user_id, room_id, timestamp, access_type,
                    access_method, access_status
                )
                VALUES (%s, %s, %s, %s, %s, %s);
            """, (access_log[1], access_log[2], access_log[3], access_log[4],
                access_log[5], access_log[6]))
        
        conn.commit()
        cur.close()
        cur_psql.close()

        print("Inserted data into pgpool from psql")
    except (Exception, psycopg2.Error) as error:
        print("Error while inserting data into pgpool: ", error)


def increase_capacity_with_lock(conn, increase_count, process_name, with_lock):
    cur = conn.cursor()
    cur.execute("BEGIN;")
    if with_lock:
        cur.execute("LOCK TABLE rooms IN EXCLUSIVE MODE;")
        print(f"EXCLUSIVE Lock acquired by {process_name} for table rooms")
    cur.execute("SELECT occupancy_limit FROM rooms WHERE room_id=1;")
    room_capacity = cur.fetchone()[0]
    new_capacity = room_capacity + increase_count
    time.sleep(5)
    print(f"Increasing room capacity by {increase_count} from {room_capacity} to {new_capacity}")
    cur.execute(f"UPDATE rooms SET occupancy_limit={new_capacity} WHERE room_id=1;")
    cur.execute("COMMIT;")
    if with_lock:
        print(f"EXCLUSIVE Lock released by {process_name} for table rooms")
    cur.close()
    

def distributed_transaction_with_lock(conn, conn2, with_lock):
    process1 = Process(target=increase_capacity_with_lock, args=(conn, 5, "Process 1", with_lock))
    process2 = Process(target=increase_capacity_with_lock, args=(conn2, 10, "Process 2", with_lock))
    
    # Start the processes
    process1.start()
    process2.start()

    # Wait for both processes to complete
    process1.join()
    process2.join()
    

def distributed_transaction_acid_rollback(conn):
    print("Showcasing rollback scenarios for Atomicity, Consistency, Isolation, Durability")
    try:
        cur = conn.cursor()
        cur.execute("BEGIN;")
        cur.execute("SELECT room_id FROM rooms WHERE room_status='Occupied';")
        occupied_room_id = cur.fetchall()[0][0]
        cur.execute("SELECT room_id FROM rooms WHERE room_status='Available';")
        available_room_id =  cur.fetchall()[0][0]
        cur.execute(f"SELECT user_id FROM access_logs WHERE room_id={occupied_room_id} ORDER BY timestamp DESC;")
        user_id = cur.fetchall()[0][0]
        print(f"user with user_id {user_id} moving from room {occupied_room_id} to room {available_room_id}")
        cur.execute(f"UPDATE rooms SET room_status='Available' WHERE room_id={occupied_room_id};")
        cur.execute(f"UPDATE rooms SET room_status='Occupied' WHERE room_id={available_room_id};")

        cur.execute("""
            INSERT INTO access_logs (
                user_id, room_id, timestamp, access_type,
                access_method, access_status
            )
            VALUES (%s, %s, %s, %s, %s, %s);
        """, (user_id, occupied_room_id, date.today(), 'Exit',
            'Card', 'Granted'))
        raise Exception("Exception during transaction")
        cur.execute("""
            INSERT INTO access_logs (
                user_id, room_id, timestamp, access_type,
                access_method, access_status
            )
            VALUES (%s, %s, %s, %s, %s, %s);
        """, (user_id, available_room_id, date.today(), 'Entry',
            'Card', 'Granted'))
        
        cur.execute("COMMIT;")
    except Exception as e:
        print(f"An error occurred: {e}")
        print("This transaction is being rolled back")
        cur.execute("ROLLBACK;")
    cur.execute(f"SELECT room_status FROM rooms WHERE room_id={occupied_room_id};")
    print(f"Room status of {occupied_room_id} is {cur.fetchall()[0][0]}")
    cur.execute(f"SELECT room_status FROM rooms WHERE room_id={available_room_id};")
    print(f"Room status of {available_room_id} is {cur.fetchall()[0][0]}")
    cur.close()  
    
def distributed_transaction_acid_successful(conn):
    print("Showcasing successful transaction block for Atomicity, Consistency, Isolation, Durability")
    try:
        cur = conn.cursor()
        cur.execute("BEGIN;")
        cur.execute("SELECT room_id FROM rooms WHERE room_status='Occupied';")
        occupied_room_id = cur.fetchall()[0][0]
        cur.execute("SELECT room_id FROM rooms WHERE room_status='Available';")
        available_room_id =  cur.fetchall()[0][0]
        cur.execute(f"SELECT user_id FROM access_logs WHERE room_id={occupied_room_id} ORDER BY timestamp DESC;")
        user_id = cur.fetchall()[0][0]
        print(f"user with user_id {user_id} moving from room {occupied_room_id} to room {available_room_id}")
        cur.execute(f"UPDATE rooms SET room_status='Available' WHERE room_id={occupied_room_id};")
        cur.execute(f"UPDATE rooms SET room_status='Occupied' WHERE room_id={available_room_id};")

        cur.execute("""
            INSERT INTO access_logs (
                user_id, room_id, timestamp, access_type,
                access_method, access_status
            )
            VALUES (%s, %s, %s, %s, %s, %s);
        """, (user_id, occupied_room_id, date.today(), 'Exit',
            'Card', 'Granted'))
        
        cur.execute("""
            INSERT INTO access_logs (
                user_id, room_id, timestamp, access_type,
                access_method, access_status
            )
            VALUES (%s, %s, %s, %s, %s, %s);
        """, (user_id, available_room_id, date.today(), 'Entry',
            'Card', 'Granted'))
        
        cur.execute("COMMIT;")
    except Exception as e:
        print(f"An error occurred: {e}")
        print("this transaction is being rolled back")
        cur.execute("ROLLBACK;")
    cur.execute(f"SELECT room_status FROM rooms WHERE room_id={occupied_room_id};")
    print(f"Room status of {occupied_room_id} is {cur.fetchall()[0][0]}")
    cur.execute(f"SELECT room_status FROM rooms WHERE room_id={available_room_id};")
    print(f"Room status of {available_room_id} is {cur.fetchall()[0][0]}")
    cur.close()

if __name__ == "__main__":
    conn = connect_pgpool_db(DB_NAME)
    conn_psql = connect_psql_db(DB_NAME)
    conn2 = connect_pgpool_db(DB_NAME)

    create_psql_tables(conn)
    create_pgpool_SB_db(conn)
    insert_psql_data(conn, conn_psql)


    conn.autocommit = False
    distributed_transaction_acid_rollback(conn)
    distributed_transaction_acid_successful(conn)
    print("Showcasing Race Conditions without lock during concurrent transactions")
    distributed_transaction_with_lock(conn, conn2, False)
    print("Showcasing resolved Race Conditions by serialization with lock during concurrent transactions")
    distributed_transaction_with_lock(conn, conn2, True)
    conn.close()