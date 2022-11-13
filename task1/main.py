import warnings
from doc_collection_processor import process_doc_collection
from elastic import connect_elasticsearch, create_indexes_if_missing


DOCUMENTS_COLLECTION_PATH = "../resources/byweb_for_course"

if __name__ == "__main__":
    warnings.filterwarnings('ignore', message="Unverified HTTPS request is being made to host*")
    es_obj = connect_elasticsearch()
    create_indexes_if_missing(es_obj)
    process_doc_collection(DOCUMENTS_COLLECTION_PATH, es_obj, need_raw=True, need_stemmed=True)
