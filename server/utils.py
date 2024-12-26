import time
from pprint import pprint
from elasticsearch import Elasticsearch

def get_es_client(max_retries:int=5,sleep_time:int=5) -> Elasticsearch:
    i = 0
    while i < max_retries:
        try:
            es = Elasticsearch("http://127.0.0.1:9200")
            client_info = es.info()
            pprint("Connecting to the Elasticserach")
            pprint(f'Cluster info: \n {client_info}')
            return es 
        except Exception:
            pprint(f'Could not connect to Elasticsearch. Retrying...')
            time.sleep(sleep_time)
            i += 1
    raise ConnectionError("Failed to connect Elasticsearch after multiple attempts.")