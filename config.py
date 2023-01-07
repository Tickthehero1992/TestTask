from os import  environ


class Config:

    ELASTIC_HOST = environ.get("ELASTIC_HOST", "127.0.0.1")
    ELASTIC_PORT = environ.get("ELASTIC_PORT", 9200)
    ELASTIC_INDEX_NAME = environ.get("ELASTIC_INDEX_NAME", "test_index")
