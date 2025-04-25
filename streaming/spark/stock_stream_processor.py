from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import psycopg2
from psycopg2.extras import execute_batch

# Define schema for stock data
stock_schema = StructType([
    StructField("symbol", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("volume", LongType(), True),
    StructField("change_percent", DoubleType(), True),
    StructField("timestamp", TimestampType(), True),
    StructField("sector", StringType(), True)
])

# Database connection parameters
DB_PARAMS = {
    "host": "localhost",
    "database": "stock_analysis",
    "user": "stock_user",
    "password": "stock123"
}

def create_result_tables():
    """Create tables for storing streaming results"""
    conn = None
    try:
        conn = psycopg2.connect(**DB_PARAMS)
        cursor = conn.cursor()
        
        # Table for moving averages
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS stock_moving_averages (
            id SERIAL PRIMARY KEY,
            symbol VARCHAR(10) NOT NULL,
            window_size VARCHAR(20) NOT NULL,
            avg_price NUMERIC(10, 2) NOT NULL,
            window_end_time TIMESTAMP NOT NULL,
            processed_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """)
        
        # Table for sector performance
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS sector_performance (
            id SERIAL PRIMARY KEY,
            sector VARCHAR(50) NOT NULL,
            avg_price NUMERIC(10, 2) NOT NULL,
            total_volume BIGINT NOT NULL,
            avg_change_percent NUMERIC(10, 2),
            window_end_time TIMESTAMP NOT NULL,
            processed_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """)
        
        # Table for market alerts
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS market_alerts (
            id SERIAL PRIMARY KEY,
            symbol VARCHAR(10) NOT NULL,
            alert_type VARCHAR(50) NOT NULL,
            value NUMERIC(10, 2) NOT NULL,
            threshold NUMERIC(10, 2) NOT NULL,
            triggered_at TIMESTAMP NOT NULL,
            processed_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """)
        
        conn.commit()
        print("Result tables created successfully")
    except Exception as e:
        print(f"Error creating result tables: {e}")
    finally:
        if conn:
            conn.close()

def save_moving_averages(batch_df, batch_id):
    """Save moving averages to PostgreSQL"""
    if not batch_df.isEmpty():
        # Convert batch to pandas and save to PostgreSQL
        pandas_df = batch_df.toPandas()
        conn = psycopg2.connect(**DB_PARAMS)
        cursor = conn.cursor()
        
        try:
            data = [
                (row['symbol'], row['window_size'], row['avg_price'], row['window_end_time'])
                for index, row in pandas_df.iterrows()
            ]
            
            execute_batch(cursor, """
            INSERT INTO stock_moving_averages (symbol, window_size, avg_price, window_end_time)
            VALUES (%s, %s, %s, %s)
            """, data)
            
            conn.commit()
            print(f"Batch {batch_id}: Saved {len(data)} moving averages")
        except Exception as e:
            print(f"Error saving batch {batch_id}: {e}")
        finally:
            cursor.close()
            conn.close()

def save_sector_performance(batch_df, batch_id):
    """Save sector performance to PostgreSQL"""
    if not batch_df.isEmpty():
        # Convert batch to pandas and save to PostgreSQL
        pandas_df = batch_df.toPandas()
        conn = psycopg2.connect(**DB_PARAMS)
        cursor = conn.cursor()
        
        try:
            data = [
                (row['sector'], row['avg_price'], row['total_volume'], 
                 row['avg_change_percent'], row['window_end_time'])
                for index, row in pandas_df.iterrows()
            ]
            
            execute_batch(cursor, """
            INSERT INTO sector_performance (sector, avg_price, total_volume, avg_change_percent, window_end_time)
            VALUES (%s, %s, %s, %s, %s)
            """, data)
            
            conn.commit()
            print(f"Batch {batch_id}: Saved {len(data)} sector performance records")
        except Exception as e:
            print(f"Error saving batch {batch_id}: {e}")
        finally:
            cursor.close()
            conn.close()

def process_stock_stream():
    """Process streaming stock data with Spark"""
    # Create SparkSession
    spark = SparkSession.builder \
        .appName("StockStreamProcessor") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2") \
        .getOrCreate()
    
    # Set log level
    spark.sparkContext.setLogLevel("WARN")
    
    # Read from Kafka
    kafka_stream = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "raw-stock-data") \
        .option("startingOffsets", "latest") \
        .load()
    
    # Parse JSON data
    parsed_stream = kafka_stream \
        .select(from_json(col("value").cast("string"), stock_schema).alias("data")) \
        .select("data.*") \
        .withWatermark("timestamp", "1 minute")  # Set watermark for late data
    
    # Register as temp view for SQL queries
    parsed_stream.createOrReplaceTempView("stock_stream")
    
    # 1. Calculate moving averages with different window sizes
    # 5-minute window
    moving_avg_5min = spark.sql("""
        SELECT 
            symbol, 
            '5min' as window_size,
            AVG(price) as avg_price,
            MAX(timestamp) as window_end_time
        FROM stock_stream
        GROUP BY 
            symbol,
            WINDOW(timestamp, '5 minutes')
    """)
    
    # 15-minute window
    moving_avg_15min = spark.sql("""
        SELECT 
            symbol, 
            '15min' as window_size,
            AVG(price) as avg_price,
            MAX(timestamp) as window_end_time
        FROM stock_stream
        GROUP BY 
            symbol,
            WINDOW(timestamp, '15 minutes')
    """)
    
    # 30-minute window
    moving_avg_30min = spark.sql("""
        SELECT 
            symbol, 
            '30min' as window_size,
            AVG(price) as avg_price,
            MAX(timestamp) as window_end_time
        FROM stock_stream
        GROUP BY 
            symbol,
            WINDOW(timestamp, '30 minutes')
    """)
    
    # Union all moving averages
    all_moving_avgs = moving_avg_5min.union(moving_avg_15min).union(moving_avg_30min)
    
    # 2. Calculate sector performance
    sector_performance = spark.sql("""
        SELECT 
            sector,
            AVG(price) as avg_price,
            SUM(volume) as total_volume,
            AVG(change_percent) as avg_change_percent,
            MAX(timestamp) as window_end_time
        FROM stock_stream
        GROUP BY 
            sector,
            WINDOW(timestamp, '15 minutes')
    """)
    
    # Write moving averages to PostgreSQL
    moving_avg_query = all_moving_avgs \
        .writeStream \
        .outputMode("append") \
        .foreachBatch(save_moving_averages) \
        .start()
    
    # Write sector performance to PostgreSQL
    sector_query = sector_performance \
        .writeStream \
        .outputMode("append") \
        .foreachBatch(save_sector_performance) \
        .start()
    
    # Wait for queries to terminate
    moving_avg_query.awaitTermination()
    sector_query.awaitTermination()

if __name__ == "__main__":
    create_result_tables()
    process_stock_stream()
