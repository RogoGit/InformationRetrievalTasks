import os
import re
import base64
import bs4 as bs
from urllib.parse import urljoin
from document import Document


def preprocess_doc_collection(collection_path):
    documents_dict = {}
    for filename in os.listdir(collection_path):
        with open(os.path.join(collection_path, filename)) as file:
            file_content = file.read()
            xml_soup = bs.BeautifulSoup(file_content, 'html.parser')
            raw_documents = xml_soup.findAll("document")
            print("Parsing document " + str(file.name) + ". Count: " + str(len(raw_documents)))
            counter = 0
            for raw_document in raw_documents:
                counter += 1
                try:
                    doc_url = decode_text(raw_document.find("docurl").text)
                    doc_content, href_list = process_doc_content(decode_text(raw_document.find("content").text), doc_url)
                    doc_id = raw_document.find("docid").text
                    doc = Document(doc_content, doc_id, doc_url, href_list)
                    documents_dict[doc_id] = doc
                    # print(doc)
                    print('Processed ' + str(counter))
                except Exception:
                    print("Error while processing " + str(raw_document.find("docid").text))
                    continue
            file.close()
    return documents_dict


def decode_text(encoded_text):
    return base64.b64decode(encoded_text).decode('cp1251')


def complete_href_relative_path(href_link, doc_url):
    if 'http://' in href_link or 'https://' in href_link:
        return href_link
    else:
        return urljoin(doc_url, href_link)


def process_doc_content(doc_text, doc_url):
    hrefs_list = []
    soup = bs.BeautifulSoup(doc_text, 'html.parser')
    for a in soup.find_all('a', href=True):
        hrefs_list.append(complete_href_relative_path(a['href'], doc_url))
    for script in soup(["script", "style"]):
        script.extract()
    processed_text = soup.get_text()
    lines = (line.strip() for line in processed_text.splitlines()) # break into lines and remove leading and trailing space on each
    chunks = (phrase.strip() for line in lines for phrase in line.split("  "))  # break multi-headlines into a line each
    processed_text = '\n'.join(chunk for chunk in chunks if chunk)  # drop blank lines
    # processed_text = re.sub("(<!--.*?-->)", "", soup.get_text(), flags=re.DOTALL) - for comments in HTML
    return processed_text, list(set(hrefs_list))
