import json
import logging

import requests

from etl import settings
from etl.shared.backoff import backoff


class ElasticService:
    def elastic_load(self, data: list):
        if not self.check_index_exists():
            self.create_index()
        self.load_data_to_elastic(data)

    @backoff(start_sleep_time=0.1, factor=2, border_sleep_time=10)
    def create_index(self):
        index_config = self.index_config()
        url = settings.ELASTIC_URL
        headers = {"Content-Type": "application/json"}
        try:
            requests.put(
                url, headers=headers, data=json.dumps(index_config), timeout=15
            )
        except Exception as e:
            logging.error(e)

    @backoff(start_sleep_time=0.1, factor=2, border_sleep_time=10)
    def load_data_to_elastic(self, data: list):
        url = settings.ELASTIC_URL + "_doc/"
        headers = {"Content-Type": "application/json"}
        try:
            requests.post(url, headers=headers, data=json.dumps(data), timeout=15)
            logging.info("Загрузиди данные в эластик")
        except Exception as e:
            logging.error(e)

    @backoff(start_sleep_time=0.1, factor=2, border_sleep_time=10)
    def index_config(self):
        with open("es_schema.txt", "r", encoding="utf8") as file:
            index_config = file.read()
        url = settings.ELASTIC_URL
        headers = {"Content-Type": "application/json"}
        try:
            requests.put(
                url, headers=headers, data=json.dumps(index_config), timeout=15
            )
            logging.info("Создали индекс")
        except Exception as e:
            logging.error(e)

    @backoff(start_sleep_time=0.1, factor=2, border_sleep_time=10)
    def check_index_exists(self):
        url = settings.ELASTIC_URL
        try:
            response = requests.get(f"{url}")
            if response.status_code == 200:
                return True
            else:
                return False
        except requests.exceptions.RequestException as e:
            logging.error(f"Ошибка при проверке индекса: {e}")
            return False
