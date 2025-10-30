import requests
from datetime import datetime
from typing import Dict, List

class DeliveryReader:
    def __init__(self) -> None:
        self.api_key = '25c27781-8fde-4b30-a22e-524044a7580f'
        self.nickname = 'mirumirvalentina'
        self.cohort = '39'
        self.base_url = 'https://d5d04q7d963eapoepsqr.apigw.yandexcloud.net/deliveries'

    def get_delivery(self, limit: int, offset: int = 0) -> List[Dict]:

        headers = {
            'X-Nickname': self.nickname,
            'X-Cohort': self.cohort,
            'X-API-KEY': self.api_key,
            'Content-Type': 'application/json'
        }

        params = {
            'sort_field': 'order_id',
            'sort_direction': 'asc',
            'limit': limit,
            'offset': offset
        }

        response = requests.get(self.base_url, headers=headers, params=params)
        response.raise_for_status()  # Проверяем на ошибки

        delivery_data = response.json()
        
        if isinstance(delivery_data, list):
            docs = delivery_data  # Присваиваем docs значение delivery_data
            ("SUCCESS:", delivery_data)
        else:
            print("Unexpected data format:", delivery_data)
            return []

        # # Пример фильтрации по update_ts, если это необходимо
        # load_threshold = datetime.now()  # Установите ваш порог времени
        # filtered_delivery = [
        #     delivery for delivery in docs
        #     if 'update_ts' in delivery and datetime.fromisoformat(delivery['update_ts']) > load_threshold
        # ]

        return docs  # Возвращаем отфильтрованные курьеры

