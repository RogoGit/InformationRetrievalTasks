import networkx as nx
import re
import sys
from elasticsearch.helpers import scan
from elastic import import_record_to_elastic


def get_all_documents_query():
    return {
        'query': {
            'match_all': {}
        }
    }


def get_all_documents_count(es_obj):
    return int(es_obj.cat.count(index="raw", params={"format": "json"})[0]['count'])


def create_documents_graph(es_obj):
    document_by_page_url = {}
    pages_graph = nx.DiGraph()
    # going to get documents from 'raw' index in elasticsearch in order to not preprocess everything again
    documents = scan(es_obj, index="raw", body=get_all_documents_query())
    processed_counter = 0
    for hit in documents:
        document_json = hit['_source']   # document data in json
        page_url = document_json['doc_url']      # document page url
        hrefs_list = document_json['hrefs_list']    # links of document
        # add document data to url ,mapping
        if page_url not in document_by_page_url:
            document_by_page_url[page_url] = document_json
        # add new node
        if page_url not in pages_graph:
            pages_graph.add_node(page_url)
        # check links
        for href in hrefs_list:
            if href not in pages_graph:
                pages_graph.add_node(href)
            pages_graph.add_edge(page_url, href)
        print(processed_counter)
        processed_counter += 1
    print('Nodes in graph: ' + str(pages_graph.number_of_nodes()))
    print('Edges in graph: ' + str(pages_graph.number_of_edges()))
    visualize_graph(pages_graph)
    return pages_graph, document_by_page_url


def count_pagerank(page_graph):
    print("Started pagerank counting")
    pagerank_counted = nx.pagerank(page_graph)
    print("Finished pagerank counting")
    return pagerank_counted


def import_pagerank_to_elastic_search(es_obj, document_by_page_url, pagerank_counted):
    print("Start importing index with pagerank")
    i = 0
    for page_url in document_by_page_url:
        doc_json = document_by_page_url[page_url]
        doc_json['pagerank'] = pagerank_counted[page_url]
        import_record_to_elastic(es_obj, doc_json, "stemmed_pagerank")
        i += 1
        print(i)
    print("Finish importing index with pagerank")


def visualize_graph(page_graph):
    nx.write_gexf(page_graph, "../resources/graph_full.gexf")
    file_contents = ""
    # fix illegal characters in resulting XML
    with open("../resources/graph_full.gexf", "r") as file:
        file_contents = file.read()
    with open("../resources/graph_full.gexf", "w") as file:
        illegal_unichrs = [(0x00, 0x08), (0x0B, 0x0C), (0x0E, 0x1F),
                           (0x7F, 0x84), (0x86, 0x9F),
                           (0xFDD0, 0xFDDF), (0xFFFE, 0xFFFF)]
        if sys.maxunicode >= 0x10000:
            illegal_unichrs.extend([(0x1FFFE, 0x1FFFF), (0x2FFFE, 0x2FFFF),
                                    (0x3FFFE, 0x3FFFF), (0x4FFFE, 0x4FFFF),
                                    (0x5FFFE, 0x5FFFF), (0x6FFFE, 0x6FFFF),
                                    (0x7FFFE, 0x7FFFF), (0x8FFFE, 0x8FFFF),
                                    (0x9FFFE, 0x9FFFF), (0xAFFFE, 0xAFFFF),
                                    (0xBFFFE, 0xBFFFF), (0xCFFFE, 0xCFFFF),
                                    (0xDFFFE, 0xDFFFF), (0xEFFFE, 0xEFFFF),
                                    (0xFFFFE, 0xFFFFF), (0x10FFFE, 0x10FFFF)])

        illegal_ranges = [fr'{chr(low)}-{chr(high)}' for (low, high) in illegal_unichrs]
        xml_illegal_character_regex = '[' + ''.join(illegal_ranges) + ']'
        illegal_xml_chars_re = re.compile(xml_illegal_character_regex)
        file.write(illegal_xml_chars_re.sub('', file_contents))
