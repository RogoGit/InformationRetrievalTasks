from doc_collection_preprocessor import preprocess_doc_collection

DOCUMENTS_COLLECTION_PATH = "../resources/byweb_for_course"

if __name__ == "__main__":
    documents_dict = preprocess_doc_collection(DOCUMENTS_COLLECTION_PATH)
    print(documents_dict)
