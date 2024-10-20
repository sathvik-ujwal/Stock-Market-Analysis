import json
import time
from kafka import KafkaProducer
import requests

# Initialize the Kafka producer
producer = KafkaProducer(bootstrap_servers='localhost:9092',
                         value_serializer=lambda v: json.dumps(v).encode('utf-8'))

# Function to fetch stock data from an API
def fetch_stock_data(symbol):
    api_url = f'https://api.example.com/stock/{symbol}/quote'  # Replace with a valid API
    response = requests.get(api_url)
    data = response.json()
    return {
        'symbol': data['symbol'],
        'price': data['latestPrice'],
        'timestamp': data['latestUpdate']
    }

# Main loop to fetch and send stock data
if __name__ == "__main__":
    stock_symbol = 'AAPL'  # Example: Apple Inc.
    
    while True:
        stock_data = fetch_stock_data(stock_symbol)
        producer.send('stock_data', stock_data)
        print(f"Sent data: {stock_data}")
        time.sleep(5)  # Fetch data every 5 seconds
