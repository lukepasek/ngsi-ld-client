from ngsildclient import ContextBrokerClient
from dotenv import load_dotenv
import os

load_dotenv()

scorpio = ContextBrokerClient(
    base_url=os.environ.get('SCORPIO_BASE_URL'),
    tenant=os.environ.get('SCORPIO_TENANT'),
    keycloak_url=os.environ.get('KEYCLOAK_URL'),    
    keycloak_realm=os.environ.get('KEYCLOAK_REALM'),
    client_id=os.environ.get('KEYCLOAK_CLIENT_ID'),
    client_secret_key=os.environ.get('KEYCLOAK_CLIENT_SECRET_KEY')
)

# list types
types_json = scorpio.get_types(print_response=True)

# list all entities by type
for long_type_name in types_json['typeList']:
    type_name, context = scorpio.sdm_type_to_context(long_type_name)
    # the following two call are quivalent from the broker perspective
    scorpio.get_entities_by_type(type_name, context, print_response=True)
    # scorpio.get_entities_by_type(long_type_name, context, print_response=True)

# --- current/last entity values ---

# get entities without context in normalised (long form) using normalised SDM type name
# entities_json = scorpio.get_entities_by_type('https://smartdatamodels.org/dataModel.Device/Device', print_response=True)

# get entities with explicitly specified context and context aware short type name
# entities_json = scorpio.get_entities_by_type('Device', context='https://raw.githubusercontent.com/smart-data-models/dataModel.Device/master/context.jsonld', print_response=True)

# get entities wuth explicitly specified context and normalised SDM type name
# entities_json = scorpio.get_entities_by_type('https://smartdatamodels.org/dataModel.Device/Device', context='https://raw.githubusercontent.com/smart-data-models/dataModel.Device/master/context.jsonld', print_response=True)

# get entities with explicitly specified context and normalised SDM type name
# entities_json = scorpio.get_entities_by_type('https://smartdatamodels.org/dataModel.Device/Device', context='https://raw.githubusercontent.com/smart-data-models/dataModel.Device/master/context.jsonld', print_response=True)


# get entities with context specified as SDM model name and normalised SDM type name
# entities_json = scorpio.get_entities_by_type('https://smartdatamodels.org/dataModel.Device/Device', sdm_model='Device', print_response=True)

# get entities with context specified as SDM model name and context aware short type name
# entities_json = scorpio.get_entities_by_type('Camera', sdm_model='Device', print_response=True)

# all of the above applies to get_entity (by id)
entity_json = scorpio.get_entity('urn:ngsi-ld:Device:macq-qsense:70b3d5e5fffe122d', sdm_model='Device', print_response=True)


# --- temporal/historic entity values ---


# scorpio.get_temporal_entities_by_type('https://smartdatamodels.org/dataModel.Weather/WeatherObserved', last_n=5, print_response=True)

# scorpio.get_temporal_entities_by_type('Camera', sdm_model='Device', last_n=3, print_response=True)

# scorpio.get_temporal_entities_by_type('ItemFlowObserved', sdm_model='Transportation', last_n=3, attrs="averageSpeed", query_params={'aggrMethods':'max,min,avg'}, print_response=True)

# scorpio.get_temporal_entities_by_type('AirQualityObserved', sdm_model='Environment', attrs='no2', to_time='2025-02-13T15:07:28.000Z', from_time='2025-02-13T14:48:05.000Z', last_n=50, print_response=True)

entity_json = scorpio.get_temporal_entity('urn:ngsi-ld:Device:macq-qsense:70b3d5e5fffe122d', sdm_model='Device', last_n=5, print_response=True)