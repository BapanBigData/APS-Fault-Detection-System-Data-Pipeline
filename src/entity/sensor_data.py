import os
import json
import pandas as pd


class SensorData:
    
    def __init__(self, record: dict) -> None:
        for k, v in record.items():
            setattr(self, k, v)
    
    @staticmethod
    def dict_to_object(data: dict):
        return SensorData(data)
    
    def to_dict(self):
        return self.__dict__
    
    @classmethod
    def get_object(cls, file_path):
        chunk_df = pd.read_csv(file_path, chunksize=10, encoding='utf-8')
        
        for df in chunk_df:
            for data in df.values:
                # Convert to strings and handle 'na' as None
                cleaned_data = {}
                for k, v in zip(df.columns, data):
                    cleaned_data[k] = None if v == 'na' else str(v)
                
                generic = SensorData(cleaned_data)
                yield generic.to_dict()
    
    @classmethod
    def generate_avro_schema(cls, file_path, record_name="SensorRecord", namespace="com.mycorp.mynamespace", 
                            avro_schema_dir='avro_schema'):
        
        # Read a chunk of the CSV file to infer schema
        df = next(pd.read_csv(file_path, chunksize=10))
        
        # Start building the schema
        schema = {
            "type": "record",
            "namespace": namespace,
            "name": record_name,
            "fields": []
        }

        # Infer the schema for each column
        for column in df.columns:
            field_type = ["null", "string"]  # Default to string type, considering "na" and other non-numeric values
            field_schema = {
                "name": column,
                "type": field_type,
                "default": None
            }
            schema["fields"].append(field_schema)
        
        # Ensure the directory exists
        if not os.path.exists(avro_schema_dir):
            os.makedirs(avro_schema_dir)
        
        # Construct the schema file path
        csv_filename = os.path.basename(file_path).replace('.csv', '.avsc')
        schema_file_path = os.path.join(avro_schema_dir, csv_filename)
        
        # Save the schema to a .avsc file
        with open(schema_file_path, 'w') as schema_file:
            json.dump(schema, schema_file, indent=2)

        print(f"Avro schema saved to {schema_file_path}")
        
        return schema
    
    