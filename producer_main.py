from src.kafka_producer.avro_producer import produce_data_to_kafka_topic
from src.constant import SAMPLE_DIR
import os 

if __name__ == '__main__':
    
    try:
        topics = os.listdir(SAMPLE_DIR)
        print(f'topics: {topics}')
        
        for topic in topics:
            sample_topic_data_dir = os.path.join(SAMPLE_DIR, topic)
            sample_file_path = os.path.join(sample_topic_data_dir, os.listdir(sample_topic_data_dir)[0])
            produce_data_to_kafka_topic(topic=topic, file_path=sample_file_path)
        
        print('\nAll data sucessfully produced to the kafka topic!!!')
        
    except Exception as e:
        raise Exception(e)
