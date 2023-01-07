import requests as req
import json
import argparse

from elasticsearch import Elasticsearch, ElasticsearchException
from elasticsearch_dsl import Search, Q
from config import Config


class IndexCreator:

    MAPPING = {
        "settings": {
            "number_of_shards": 1,
            "number_of_replicas": 1
        },
        "mappings":
            {
                "_doc":
                    {
                        "properties": {
                            "@timestamp": {"format": "dateOptionalTime", "type": "date"},
                            "value": {"type": "integer"}
                        }
                    }
            }
    }

    def __init__(self, path):

        self.path = path
        self.es_url = f"http://{Config.ELASTIC_HOST}:{Config.ELASTIC_PORT}"
        self.es = Elasticsearch(self.es_url)

    def parse_file(self):
        with open(self.path) as fp:
            df = json.load(fp)
        return df

    def fill_index(self):


        if req.get(self.es_url+"/"+Config.ELASTIC_INDEX_NAME).status_code == 404:
            self.es.indices.create(index=Config.ELASTIC_INDEX_NAME,
                                   body=self.MAPPING)
        searcher = Search(using=self.es, index=Config.ELASTIC_INDEX_NAME)

        s = searcher.query()
        df = self.parse_file()
        for d in df:
            query = {
                "query":
                    {
                        "bool": {
                            "must": [
                                {"match": {"@timestamp": d["@timestamp"]}},
                                {"match": {"value": d["value"]}}
                            ]
                        }
                    }
            }

            response = self.es.search(index=Config.ELASTIC_INDEX_NAME, body=query)
            try:
                hits = [hit for hit in response['hits']['hits']]
                hits_correct = [hit['_source'] for hit in hits]

            except ElasticsearchException:
                pass
                raise

            if len(hits_correct):
                pass
            else:
                print(d)
                self.es.index(index=Config.ELASTIC_INDEX_NAME, body=d)



parser = argparse.ArgumentParser()
parser.add_argument("--path", dest="path", help="Путь создания файла", default="data/test.json")


if __name__ == "__main__":
    args = parser.parse_args()
    ci = IndexCreator(path=args.path)
    ci.fill_index()


