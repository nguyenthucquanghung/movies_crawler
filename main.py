import requests
from elasticsearch import Elasticsearch


def get_cookie():
    # Login
    login_url = 'https://movielens.org/api/sessions'
    payload = {'userName': "thienanh", 'password': "thienanh1"}
    res = requests.post(login_url, json=payload)
    return res.headers['Set-Cookie'].split(';')[0].split('=')


if __name__ == '__main__':
    f = open('movies.csv', encoding='utf-8')
    data = f.read()
    films = data.split('\n')

    # Get film
    es = Elasticsearch([{'host': '0.0.0.0', 'port': 9200}])
    es_index_name = 'movies'
    cookie = get_cookie()
    base_url = 'https://movielens.org/api/movies/'
    n = len(films)
    n_fail = 0
    for i in range(40123, n):
        film_data = films[i].split(',')
        film_id = film_data[0]
        film_title = film_data[1]
        film_genre = film_data[2]
        try:
            response = requests.get(base_url + film_id, cookies={cookie[0]: cookie[1]})
            if response.json()['status'] == 'error':
                cookie = get_cookie()
                response = requests.get(base_url + film_id, cookies={cookie[0]: cookie[1]})
            if response.json()['status'] == 'fail':
                n_fail += 1
            else:
                doc_movie = response.json()['data']['movieDetails']['movie']
                id_movie = response.json()['data']['movieDetails']['movieId']
                es_resp = es.index(index=es_index_name, id=id_movie, body=doc_movie)
                if es_resp["_shards"]["successful"] == 1:
                    print('Process: ' + str(i) + '/' + str(n - 1) + '; Fail: ' + str(n_fail))
                else:
                    n_fail += 1
                    print(response)
                    print(es_resp)
                    print('Process: ' + str(i) + '/' + str(n - 1) + '; Fail: ' + str(n_fail))
        except Exception:
            cookie = get_cookie()
            response = requests.get(base_url + film_id, cookies={cookie[0]: cookie[1]})
            if response.json()['status'] == 'error':
                cookie = get_cookie()
                response = requests.get(base_url + film_id, cookies={cookie[0]: cookie[1]})
                if response.json()['status'] == 'fail':
                    n_fail += 1
                else:
                    doc_movie = response.json()['data']['movieDetails']['movie']
                    id_movie = response.json()['data']['movieDetails']['movieId']
                    es_resp = es.index(index=es_index_name, id=id_movie, body=doc_movie)
                    if es_resp["_shards"]["successful"] == 1:
                        print('Process: ' + str(i) + '/' + str(n - 1) + '; Fail: ' + str(n_fail))
                    else:
                        n_fail += 1
                        print(response)
                        print(es_resp)
                        print('Process: ' + str(i) + '/' + str(n - 1) + '; Fail: ' + str(n_fail))
