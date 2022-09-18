import os
import re
import base64
import bs4 as bs
from document import Document


def preprocess_doc_collection(collection_path):
    documents_dict = {}
    for filename in os.listdir(collection_path):
        with open(os.path.join(collection_path, filename)) as file:
            file_content = file.read()
            xml_soup = bs.BeautifulSoup(file_content, 'html.parser')
            raw_documents = xml_soup.findAll("document")
            for raw_document in raw_documents:
                doc_content, href_list = process_doc_content(decode_text(raw_document.find("content").text))
                doc_url = decode_text(raw_document.find("docurl").text)
                doc_id = raw_document.find("docid").text
                doc = Document(doc_content, doc_id, doc_url, href_list)
                # print(doc)
                documents_dict[doc_id] = doc
            file.close()
    return documents_dict


def decode_text(encoded_text):
    return base64.b64decode(encoded_text).decode('cp1251')


def process_doc_content(doc):
    hrefs_list = []
    soup = bs.BeautifulSoup(doc, 'html.parser')
    for a in soup.find_all('a', href=True):
        hrefs_list.append(a['href'])
    for script in soup(["script", "style"]):
        script.extract()
    processed_text = soup.get_text()
    lines = (line.strip() for line in processed_text.splitlines()) # break into lines and remove leading and trailing space on each
    chunks = (phrase.strip() for line in lines for phrase in line.split("  "))  # break multi-headlines into a line each
    processed_text = '\n'.join(chunk for chunk in chunks if chunk)  # drop blank lines
    # processed_text = re.sub("(<!--.*?-->)", "", soup.get_text(), flags=re.DOTALL) - for comments in HTML
    return processed_text, list(set(hrefs_list))
