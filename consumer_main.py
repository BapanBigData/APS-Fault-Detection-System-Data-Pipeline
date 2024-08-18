import argparse
import os
from src.kafka_consumer.avro_consumer import mongodb_consumer
from src.constant import SAMPLE_DIR

if __name__ == '__main__':
    try:
        # Set up argument parsing
        parser = argparse.ArgumentParser(description='Kafka Consumer for MongoDB')
        
        parser.add_argument('--database', required=True, help='The MongoDB database name')
        
        args = parser.parse_args()
        
        # Extract the command-line arguments
        database = args.database
        
        # List topics from the SAMPLE_DIR
        topics = os.listdir(SAMPLE_DIR)
        print(f'Topics: {topics}...')
        
        # Consume from each topic and insert into MongoDB
        for topic in topics:
            mongodb_consumer(topic=topic, database=database, collection_name=topic)
        
    except Exception as e:
        raise Exception(e)
