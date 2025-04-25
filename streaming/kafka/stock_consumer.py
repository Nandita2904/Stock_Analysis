import json
import psycopg2
from kafka import KafkaConsumer

# Database connection parameters
DB_PARAMS = {
    "host": "localhost",
    "database": "stock_analysis",
    "user": "stock_user",
    "password": "stock123"
}

def create_db_table():
    """Create the necessary table if it doesn't exist"""
    conn = None
    try:
        conn = psycopg2.connect(**DB_PARAMS)
        cursor = conn.cursor()
        
        # Create table for raw stock data
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS raw_stock_data (
            id SERIAL PRIMARY KEY,
            symbol VARCHAR(10) NOT NULL,
            price NUMERIC(10, 2) NOT NULL,
            volume BIGINT NOT NULL,
            change_percent NUMERIC(10, 2),
            timestamp TIMESTAMP NOT NULL,
            sector VARCHAR(50),
            processed_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """)
        
        conn.commit()
        print("Database table created successfully")
    except Exception as e:
        print(f"Error creating database table: {e}")
    finally:
        if conn:
            conn.close()

def consume_and_store():
    """Consume messages from Kafka and store in PostgreSQL"""
    # Create Kafka consumer
    consumer = KafkaConsumer(
        'raw-stock-data',
        bootstrap_servers=['localhost:9092'],
        auto_offset_reset='earliest',
        value_deserializer=lambda m: json.loads(m.decode('utf-8'))
    )
    
    conn = None
    try:
        conn = psycopg2.connect(**DB_PARAMS)
        cursor = conn.cursor()
        
        print("Starting stock data consumer...")
        for message in consumer:
            stock_data = message.value
            
            # Insert data into PostgreSQL
            cursor.execute("""
            INSERT INTO raw_stock_data (symbol, price, volume, change_percent, timestamp, sector)
            VALUES (%s, %s, %s, %s, %s, %s)
            """, (
                stock_data['symbol'],
                stock_data['price'],
                stock_data['volume'],
                stock_data['change_percent'],
                stock_data['timestamp'],
                stock_data['sector']
            ))
            
            conn.commit()
            print(f"Stored: {stock_data}")
            
    except Exception as e:
        print(f"Error: {e}")
    finally:
        if conn:
            conn.close()
        consumer.close()

if __name__ == "__main__":
    create_db_table()
    consume_and_store()
