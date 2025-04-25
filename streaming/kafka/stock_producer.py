import json
import time
import random
import requests
from datetime import datetime
from kafka import KafkaProducer

# Create Kafka producer
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Sample stock symbols
stock_symbols = ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'META', 'TSLA', 'NVDA', 'JPM', 'BAC', 'DIS']

# Stock sectors mapping
sectors = {
    'AAPL': 'Technology', 'MSFT': 'Technology', 'GOOGL': 'Technology', 
    'AMZN': 'Consumer Cyclical', 'META': 'Technology', 'TSLA': 'Automotive',
    'NVDA': 'Technology', 'JPM': 'Financial Services', 'BAC': 'Financial Services',
    'DIS': 'Entertainment'
}

# Function to fetch real stock data from Alpha Vantage
def fetch_stock_data(symbol):
    # Replace with your API key
    api_key = "YOUR_ALPHA_VANTAGE_API_KEY"
    url = f"https://www.alphavantage.co/query?function=GLOBAL_QUOTE&symbol={symbol}&apikey={api_key}"
    
    try:
        response = requests.get(url)
        data = response.json()
        if "Global Quote" in data:
            quote = data["Global Quote"]
            return {
                "symbol": symbol,
                "price": float(quote["05. price"]),
                "volume": int(quote["06. volume"]),
                "change_percent": quote["10. change percent"].strip('%'),
                "timestamp": datetime.now().isoformat(),
                "sector": sectors[symbol]
            }
    except Exception as e:
        print(f"Error fetching data for {symbol}: {e}")
    
    # Return simulated data if API call fails
    return generate_mock_stock_data(symbol)

# Function to generate mock stock data for testing
def generate_mock_stock_data(symbol):
    base_prices = {
        'AAPL': 150, 'MSFT': 300, 'GOOGL': 2500, 'AMZN': 3300, 
        'META': 300, 'TSLA': 800, 'NVDA': 500, 'JPM': 150, 
        'BAC': 40, 'DIS': 180
    }
    
    base_price = base_prices.get(symbol, 100)
    price = base_price * (1 + random.uniform(-0.05, 0.05))
    volume = random.randint(1000, 1000000)
    change_percent = random.uniform(-5, 5)
    
    return {
        "symbol": symbol,
        "price": round(price, 2),
        "volume": volume,
        "change_percent": round(change_percent, 2),
        "timestamp": datetime.now().isoformat(),
        "sector": sectors[symbol]
    }

def produce_stock_data(use_real_data=False):
    """
    Continuously produces stock data to the Kafka topic.
    Set use_real_data=True to use Alpha Vantage API (requires API key)
    """
    while True:
        for symbol in stock_symbols:
            if use_real_data:
                stock_data = fetch_stock_data(symbol)
            else:
                stock_data = generate_mock_stock_data(symbol)
            
            # Send data to Kafka topic
            producer.send('raw-stock-data', value=stock_data)
            print(f"Produced: {stock_data}")
        
        # Wait before producing next batch
        time.sleep(5)

if __name__ == "__main__":
    try:
        print("Starting stock data producer...")
        produce_stock_data(use_real_data=False)  # Set to True to use real API data
    except KeyboardInterrupt:
        print("Stopping producer")
        producer.close()
