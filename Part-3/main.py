import psycopg2 
import time

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



def indexing(conn):
    try:

        # Create a new cursor object
        cur = conn.cursor()

        # Calulating time taken to run query "Query all buildings containing at least one floor with more than 4 rooms"
        t1 = time.time()
        cur.execute('''
            EXPLAIN SELECT DISTINCT b.building_id, b.building_name
            FROM buildings b
            JOIN floors f ON b.building_id = f.building_id
            JOIN rooms r ON f.floor_id = r.floor_id
            GROUP BY b.building_id, b.building_name
            HAVING COUNT(DISTINCT f.floor_id) > 0 AND COUNT(DISTINCT r.room_id) > 4;
        ''')
        rows = cur.fetchall()
        t2 = time.time()
        print("Time taken to run query 'Query all buildings containing at least one floor with more than 4 rooms': ",  (t2-t1)*1000, "ms")
            
        
        print("Creating Indexes on Rooms Table....")
        # Create index on rooms table
        cur.execute("CREATE INDEX room_index on rooms(room_id,floor_id);")

        print("Creating Indexes on Floors Table....")
        # Create index on floors table
        cur.execute("CREATE INDEX floor_index on floors(floor_id,building_id);")

        print("Creating Indexes on Buildings Table....")
        # Create index on buildings table
        cur.execute("CREATE INDEX building_index on buildings(building_id);")

        # Calulating time taken to run query "Query all buildings containing at least one floor with more than 4 rooms"

        t1 = time.time()
        cur.execute('''
            EXPLAIN SELECT DISTINCT b.building_id, b.building_name
            FROM buildings b
            JOIN floors f ON b.building_id = f.building_id
            JOIN rooms r ON f.floor_id = r.floor_id
            GROUP BY b.building_id, b.building_name
            HAVING COUNT(DISTINCT f.floor_id) > 0 AND COUNT(DISTINCT r.room_id) > 4;
        ''')

        rows = cur.fetchall()
        t2 = time.time()
        print("Time taken to run query 'Query all buildings containing at least one floor with more than 4 rooms': ", (t2-t1)*1000, "ms")

        # Close the cursor and the connection
        cur.close()
    
    except (Exception, psycopg2.Error) as error:
        print("Error while creating indexes: ", error)


if __name__ == "__main__":
    conn = connect_psql_db(DB_NAME)
    indexing(conn)
    conn.close()