import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()


# SECURITY_PROTOCOL="SASL_SSL"
# SSL_MACHENISM="PLAIN"
# API_KEY = os.getenv('API_KEY',None)
# API_SECRET_KEY = os.getenv('API_SECRET_KEY',None)

ENDPOINT_SCHEMA_URL  = os.getenv('SCHEMA_REGISTRY_URL')
BOOTSTRAP_SERVER = os.getenv('BOOTSTRAP_SERVER')

# SECURITY_PROTOCOL = os.getenv('SECURITY_PROTOCOL',None)
# SSL_MACHENISM = os.getenv('SSL_MACHENISM',None)
# SCHEMA_REGISTRY_API_KEY = os.getenv('SCHEMA_REGISTRY_API_KEY',None)
# SCHEMA_REGISTRY_API_SECRET = os.getenv('SCHEMA_REGISTRY_API_SECRET',None)


def sasl_config():

    sasl_conf = {
        'bootstrap.servers':BOOTSTRAP_SERVER
    }
    
    return sasl_conf


def schema_config():
    return {
        'url':ENDPOINT_SCHEMA_URL
    }
