import json
import time
from kafka import KafkaProducer
import requests


producer = KafkaProducer(bootstrap_servers='localhost:9092',
                         value_serializer=lambda v: json.dumps(v).encode('utf-8'))

def fetch_stock_data(symbol):
    api_url = f'https://api.example.com/stock/{symbol}/quote'  
    response = requests.get(api_url)
    data = response.json()
    return {
        'symbol': data['symbol'],
        'price': data['latestPrice'],
        'timestamp': data['latestUpdate']
    }


if __name__ == "__main__":
    stock_symbol = 'AAPL'  
    
    while True:
        stock_data = fetch_stock_data(stock_symbol)
        producer.send('stock_data', stock_data)
        print(f"Sent data: {stock_data}")
        time.sleep(5)  
