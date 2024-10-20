import json
from kafka import KafkaConsumer
from hdfs import InsecureClient # type: ignore

# Initialize Kafka consumer
consumer = KafkaConsumer('stock_data',
                         bootstrap_servers='localhost:9092',
                         value_deserializer=lambda x: json.loads(x.decode('utf-8')),
                         auto_offset_reset='earliest',
                         enable_auto_commit=True,
                         group_id='stock_data_group')

# Initialize HDFS client
hdfs_client = InsecureClient('http://localhost:9870', user='hadoop')

# File path to store the data
hdfs_file_path = '/path/to/hdfs/stock_data.json'  # Change to your HDFS path

# Main loop to consume and store data
if __name__ == "__main__":
    for message in consumer:
        stock_data = message.value
        print(f"Consumed data: {stock_data}")
        
        # Store data in HDFS
        with hdfs_client.write(hdfs_file_path, append=True) as writer:
            writer.write(json.dumps(stock_data) + '\n')  # Write each record in a new line
