import warnings
from doc_collection_processor import process_doc_collection
from elastic import connect_elasticsearch, create_indexes_if_missing
from search_evaluator import evaluate_search
from pagerank import create_documents_graph, count_pagerank, import_pagerank_to_elastic_search


DOCUMENTS_COLLECTION_PATH = "../resources/byweb_for_course"
SEARCH_EVALUATION_RESULTS_PATH = "../resources/evaluation_metrics.txt"

if __name__ == "__main__":
    warnings.filterwarnings('ignore', message="Unverified HTTPS request is being made to host*")
    es_obj = connect_elasticsearch()
    create_indexes_if_missing(es_obj)
    process_doc_collection(DOCUMENTS_COLLECTION_PATH, es_obj, need_raw=True, need_stemmed=True)
    evaluate_search(es_obj, SEARCH_EVALUATION_RESULTS_PATH)
    documents_graph, document_by_page_url = create_documents_graph(es_obj)
    pagerank_counted = count_pagerank(documents_graph)
    import_pagerank_to_elastic_search(es_obj, document_by_page_url, pagerank_counted)
