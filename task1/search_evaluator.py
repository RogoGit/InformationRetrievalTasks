import xml.etree.ElementTree as ET


TASKS_PATH = '../resources/web2008_adhoc.xml'
RELEVANT_DOCS_PATH = '../resources/relevant_table_2009.xml'
K = 20


def get_search_query(text, size):
    return {
        "from": 0, "size": size,
        "query": {
            "match": {
                "content": {
                    "query": text
                }
            }
        }
    }


def get_search_query_with_pagerank(text, size):
    return {
        "from": 0, "size": size,
        "query": {
            "bool": {
                "must": {
                    "match": {"content": text}
                },
                "should": {
                    "rank_feature": {
                        "field": "pagerank",
                        "saturation": {
                            "pivot": 10
                        }
                    }
                }
            }
        }
    }


def count_metrics_for_task(es_obj, index_name, query_text, retrieved_documents, relevant_documents_lookup, need_pagerank=False):
    true_relevant = 0
    total_relevant_docs_count = len([doc_info for doc_info in relevant_documents_lookup
                                     if relevant_documents_lookup[doc_info] == 'vital'])

    for doc_id in retrieved_documents:
        if doc_id in relevant_documents_lookup and relevant_documents_lookup[doc_id] == 'vital':
            true_relevant += 1
    p_k = true_relevant/K
    # print("p@20: " + str(p_k))
    r_k = round(true_relevant/total_relevant_docs_count, 2) if total_relevant_docs_count != 0 else 0.0
    # print("r@20: " + str(r_k))

    if need_pagerank:
        r_precision_search_result = es_obj.search(index=index_name, body=get_search_query_with_pagerank(query_text, total_relevant_docs_count))
    else:
        r_precision_search_result = es_obj.search(index=index_name, body=get_search_query(query_text, total_relevant_docs_count))
    r_precision_retrieved_documents_ids = [result['_source']['doc_id'] for result in r_precision_search_result['hits']['hits']]
    r_precision_relevant = 0
    for doc_id in r_precision_retrieved_documents_ids:
        if doc_id in relevant_documents_lookup and relevant_documents_lookup[doc_id] == 'vital':
            r_precision_relevant += 1
    r_precision = round(r_precision_relevant/total_relevant_docs_count, 2) if total_relevant_docs_count != 0 else 0.0
    # print("R-precision: " + str(r_precision))
    return [p_k, r_k, r_precision]


def evaluate_search(es_obj, results_path):
    raw_results_dict = {}
    stemmed_results_dict = {}
    stemmed_pagerank_results_dict = {}
    search_tasks, relevant_docs_for_tasks = parse_xml_files()
    print("Start evaluation")
    for task in search_tasks:
        print(search_tasks[task])
        raw_search_task_result = es_obj.search(index="raw", body=get_search_query(search_tasks[task], K))
        stemmed_search_task_result = es_obj.search(index="stemmed", body=get_search_query(search_tasks[task], K))
        pagerank_search_task_result = es_obj.search(index="stemmed_pagerank", body=get_search_query_with_pagerank(search_tasks[task], K))
        raw_retrieved_documents_ids = [result['_source']['doc_id'] for result in raw_search_task_result['hits']['hits']]
        stemmed_retrieved_documents_ids = [result['_source']['doc_id']
                                           for result in stemmed_search_task_result['hits']['hits']]
        pagerank_retrieved_documents_ids = [result['_source']['doc_id']
                                           for result in pagerank_search_task_result['hits']['hits']]
        raw_results_dict[search_tasks[task]] = \
            count_metrics_for_task(es_obj, "raw", search_tasks[task], raw_retrieved_documents_ids, relevant_docs_for_tasks[task])
        stemmed_results_dict[search_tasks[task]] = \
            count_metrics_for_task(es_obj, "stemmed", search_tasks[task], stemmed_retrieved_documents_ids, relevant_docs_for_tasks[task])
        stemmed_pagerank_results_dict[search_tasks[task]] = \
            count_metrics_for_task(es_obj, "stemmed_pagerank", search_tasks[task], pagerank_retrieved_documents_ids,
                                   relevant_docs_for_tasks[task], True)
    map_k_raw = round(sum([raw_results_dict[task][0] for task in raw_results_dict]) / len(raw_results_dict), 3)
    map_k_stemmed = round(sum([stemmed_results_dict[task][0] for task in stemmed_results_dict]) / len(stemmed_results_dict), 3)
    map_k_pagerank = round(
        sum([stemmed_pagerank_results_dict[task][0] for task in stemmed_pagerank_results_dict]) / len(stemmed_pagerank_results_dict), 3)
    results_file = open(results_path, "w")
    results_file.write('Raw index MAP@20: {}. Stemmed index MAP@20: {}. Stemmed index with pagerank MAP@20: {}\n'
                       .format(map_k_raw, map_k_stemmed, map_k_pagerank))
    results_file.write('Task    p@20(raw/stemmed/stemmed_pagerank)   r@20(raw/stemmed/stemmed_pagerank)   r-precision(raw/stemmed/stemmed_pagerank)\n')
    for task in raw_results_dict:
        results_file.write('{}  {}/{}/{}   {}/{}/{}   {}/{}/{}\n'.format(task,
                                                                         raw_results_dict[task][0], stemmed_results_dict[task][0], stemmed_pagerank_results_dict[task][0],
                                                                         raw_results_dict[task][1], stemmed_results_dict[task][1], stemmed_pagerank_results_dict[task][1],
                                                                         raw_results_dict[task][2], stemmed_results_dict[task][2], stemmed_pagerank_results_dict[task][2]))
    results_file.close()


def parse_xml_files():
    tasks_tree = ET.parse(TASKS_PATH)
    tasks_root = tasks_tree.getroot()
    relevant_docs_tree = ET.parse(RELEVANT_DOCS_PATH)
    relevant_docs_root = relevant_docs_tree.getroot()
    search_tasks = {}
    for child in tasks_root[1:]:
        search_tasks[child.attrib.get('id')] = child[0].text
    relevant_docs_for_tasks = {}
    for task in relevant_docs_root:
        if task.attrib.get('id') in search_tasks:
            docs_dict = {}
            for document in task:
                docs_dict[document.attrib.get('id')] = document.attrib.get('relevance')
            relevant_docs_for_tasks[task.attrib.get('id')] = docs_dict
    search_tasks_only_with_documents_present = {key: search_tasks[key] for key in relevant_docs_for_tasks.keys()}
    return search_tasks_only_with_documents_present, relevant_docs_for_tasks
