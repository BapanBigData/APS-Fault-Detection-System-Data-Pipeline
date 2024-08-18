import time
from uuid import uuid4
from typing import List
from confluent_kafka import SerializingProducer
from confluent_kafka.serialization import StringSerializer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from src.kafka_config import sasl_config, schema_config
from src.kafka_logger import logging
from src.entity.sensor_data import SensorData



def delivery_report(err, msg):
    """
    Reports the success or failure of a message delivery.
    Args:
        err (KafkaError): The error that occurred on None on success.
        msg (Message): The message that was produced or failed.
    """

    if err is not None:
        logging.info("Delivery failed for User record {}: {}".format(msg.key(), err))
        print(f"Delivery failed for User record {msg.key()}: {err}")
        return
    
    logging.info('User record {} successfully produced to {} [{}] at offset {}'.format(
        msg.key(), msg.topic(), msg.partition(), msg.offset()))
    
    print(f'User record {msg.key()} successfully produced to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}')
    
    return


def produce_data_to_kafka_topic(topic, file_path):
    logging.info(f"Topic: {topic} file_path:{file_path}")
    
    schema_registry_conf = schema_config()
    schema_registry_client = SchemaRegistryClient(schema_registry_conf)
    key_serializer = StringSerializer('utf_8')
    
    # Fetch the latest Avro schema for the value
    SUBJECT_NAME = f'{topic}-value'
    schema_str = schema_registry_client.get_latest_version(SUBJECT_NAME).schema.schema_str
    
    avro_serializer = AvroSerializer(schema_registry_client, schema_str)
    
    # Define the SerializingProducer
    producer = SerializingProducer({
            'bootstrap.servers': sasl_config()['bootstrap.servers'],
            'key.serializer': key_serializer,      # Key will be serialized as a string
            'value.serializer': avro_serializer    # Value will be serialized as Avro
            })

    print(f"Producing user records to topic {topic}. ^C to exit.")
    producer.poll(1.0)
    
    try:
        for instance in SensorData.get_object(file_path=file_path):
            #cleaned_data = instance.clean_data()  # Ensure data is clean
            logging.info(f"Topic: {topic} file_path:{instance}")
            
            print(f"Processing instance: {instance}")
            
            producer.produce(topic=topic,
                            key=str(uuid4()),
                            value=instance,
                            on_delivery=delivery_report)
            
            producer.flush()  # Ensure all outstanding produce requests have completed
            
            print("\nRecord successfully flushed to Kafka!")   #This message confirms the flush is complete

            #time.sleep(1)  # wait for 1 sec 
            
    except KeyboardInterrupt:
        pass
    except ValueError:
        print("Invalid input, discarding record...")
        pass

