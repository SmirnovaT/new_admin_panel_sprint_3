from elasticsearch import Elasticsearch, ConnectionError, RequestError

from elasticsearch.helpers import bulk

from etl import settings
from etl.settings import logger
from etl.shared.backoff import backoff


class ElasticService:
    "Сервис создания индекса в эластике и загрузка данных в него"

    @backoff(start_sleep_time=0.1, factor=2, border_sleep_time=10)
    def elastic_load(self, data: list):
        es = self.connect_elasticsearch()
        if es is None:
            logger.error("Не удалось подключиться к Elasticsearch")
            return None
        else:
            actions = [
                {
                    "_index": settings.INDEX_NAME,
                    "_id": document["id"],
                    "_source": document,
                }
                for document in data
            ]
            el_update = bulk(es, actions)
            logger.info("Загрузили пачку данных в эластик")
            return el_update

    @backoff(start_sleep_time=0.1, factor=2, border_sleep_time=10)
    def create_index(self, es, index_name):
        with open("es_schema.txt", "r", encoding="utf8") as file:
            index_body = file.read()
            es.indices.create(index=index_name, body=index_body)
            logger.info(f"Индекс '{index_name}' создан.")

    @backoff(start_sleep_time=0.1, factor=2, border_sleep_time=10)
    def connect_elasticsearch(self):
        try:
            es = Elasticsearch(settings.ELASTIC_URL)
            index_name = settings.INDEX_NAME
            if not es.indices.exists(index=index_name):
                self.create_index(es, index_name)
            else:
                logger.info(f"Индекс '{index_name}' уже существует.")
            return es
        except ConnectionError as e:
            logger.error(f"Ошибка соединения с Elasticsearch: {e}")
            return None
        except RequestError as e:
            logger.error(f"Ошибка запроса Elasticsearch: {e}")
            return None
        except Exception as e:
            logger.error(f"Непредвиденная ошибка: {e}")
            return None
