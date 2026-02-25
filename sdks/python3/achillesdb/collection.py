class CollectionImpl:
    def __init__(
        self,
        id,
        name,
        database,
        http_client,
        embedding_function,
        logger,
    ):
        ...

    def count(self):
        ...

    def addDocuments(self, documents, opts=None):
        ...

    def getDocuments(self, opts=None):
        ...

    def updateDocument(self, document, opts=None):
        ...

    def updateDocuments(self, documents, opts=None):
        ...

    def deleteDocuments(self, documents, opts=None):
        ...

    def query(self, query, opts=None):
        ...

    def peek(self, n=None):
        ...
