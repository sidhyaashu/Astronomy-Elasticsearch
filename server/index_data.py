from typing import List
from utils import get_es_client
from elasticsearch import Elasticsearch
from config import INDEX_NAME
from pprint import pprint
from tqdm import tqdm
import json


def index_data(documents:List[dict]):
    es = get_es_client(max_retries=5,sleep_time=5)
    _=_create_index(es=es, index_name = INDEX_NAME)
    _=_insert_documents(es=es, index_name = INDEX_NAME,documents=documents)
    pprint(f'Indexed {len(documents)} documents into Index "{INDEX_NAME}"')


def _create_index(es:Elasticsearch) -> dict:
    es.indices.delete(index=INDEX_NAME,ignore_unavailable=True)
    return es.indices.create(index=INDEX_NAME)

def _insert_documents(es:Elasticsearch,documents:List[dict]) -> dict :
    operations = []
    for document in tqdm(documents,total=len(documents),desc="Indexing Documents"):
        operations.append({'index':{'_index':INDEX_NAME}})
        operations.append(document)
    
    return es.bulk(operations=operations)


if __name__ == '__main__':
    with open('../data/apod.json') as f:
        documents = json.load(f)

    index_data(documents=documents, use_n_gram_tokenizer=True)