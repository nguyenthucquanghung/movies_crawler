import logging
from elasticsearch import Elasticsearch


def connect_elasticsearch():
    _es = None
    _es = Elasticsearch([{'host': 'elasticsearch', 'port': 9200}])
    if _es.ping():
        print('Yay Connect')
    else:
        print('Awww it could not connect!')
    return _es


if __name__ == '__main__':
    logging.basicConfig(level=logging.ERROR)
    connect_elasticsearch()
    f = open('movies.csv', encoding='utf-8')
    data = f.read()
    films = data.split('\n')
    print(len(films))
