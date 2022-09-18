class Document:
    def __init__(self, content, doc_id, doc_url, hrefs_list):
        self.content = content
        self.doc_id = doc_id
        self.doc_url = doc_url
        self.hrefs_list = hrefs_list

    def __str__(self):
        return f"id:{self.doc_id}\n url:{self.doc_url}\n hrefs:{self.hrefs_list}\n content:\n{self.content}"
