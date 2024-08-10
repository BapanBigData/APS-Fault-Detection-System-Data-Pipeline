import os
from confluent_kafka import DeserializingConsumer, KafkaException, KafkaError
from confluent_kafka.serialization import StringDeserializer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from src.kafka_config import sasl_config, schema_config
from src.database.mongodb import MongodbOperation



def mongodb_consumer(topic, database, collection_name):

    kafka_conf = sasl_config()
    
    kafka_conf.update({
        'group.id': 'group1',
        'auto.offset.reset': "earliest"
    })
    
    
    schema_registry_url = schema_config()['url']
    
    # Create a Schema Registry client
    schema_registry_client = SchemaRegistryClient({
        'url': schema_registry_url                  # Adjust to your Schema Registry URL
    })
    
    # Fetch the latest Avro schema for the value
    SUBJECT_NAME = f'{topic}-value'
    
    schema_str = schema_registry_client.get_latest_version(SUBJECT_NAME).schema.schema_str

    # Define Avro Deserializer for the value
    key_deserializer = StringDeserializer('utf_8')
    avro_deserializer = AvroDeserializer(schema_registry_client, schema_str)

    # Define the DeserializingConsumer
    consumer = DeserializingConsumer({
        'bootstrap.servers': kafka_conf['bootstrap.servers'],
        'key.deserializer': key_deserializer,
        'value.deserializer': avro_deserializer,
        'group.id': kafka_conf['group.id'],
        'auto.offset.reset': kafka_conf['auto.offset.reset']
    })

    # Subscribe to the topic
    consumer.subscribe([topic])

    mongodb_operation = MongodbOperation(database)
    
    while True:
        try:
            msg = consumer.poll(1.0)  

            if msg is None:
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # End of partition event
                    print('%% %s [%d] reached end at offset %d\n' % (msg.topic(), msg.partition(), msg.offset()))
                    
                elif msg.error():
                    raise KafkaException(msg.error())
            else:
                # Proper message
                record = msg.value()
                
                if record is not None:
                    mongodb_operation.insert(collection_name, record)
                    
                    print(f"Inserted record into MongoDB: {record}")
                    
        except KeyboardInterrupt:
            break

    consumer.close()
