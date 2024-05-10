from logging import Logger
from typing import Dict, List
from urllib.parse import urljoin

import requests
import copy

class Connector:
    def __init__(self, host: str, api_key: str, nickname: str, cohort: str) -> None:
        self.host = host
        self.api_key = api_key
        self.nickname = nickname
        self.cohort = cohort

        self._headers={'X-Api-Key': self.api_key,
                    'X-Nickname': self.nickname,
                    'X-Cohort': self.cohort
                    }
        
        self.url = None
        self.params = {}
    
    def get(self, start_params: dict):
        params = copy.deepcopy(start_params)
        url = self.url
        data = []
        indx = 0
        has_data = True

        while has_data:
            response = requests.get(url, params=params, headers=self.headers)
            payload = response.json()
            data.extend(payload)
            payload_len = len(payload)
            params['offset'] += payload_len
            has_data = payload_len > 0
            indx += 1
       
        return payload
        
    @property
    def headers(self):
        return self._headers


class RestaurantsAPI(Connector):
    def __init__(self, host: str, api_key: str, nickname: str, cohort: str) -> None:
        super().__init__(host, api_key, nickname, cohort)
        self.url = urljoin(self.host, 'restaurants')


class CouriersAPI(Connector):
    def __init__(self, host: str, api_key: str, nickname: str, cohort: str) -> None:
        super().__init__(host, api_key, nickname, cohort)
        self.url = urljoin(self.host, 'couriers')
    
class DeliveriesAPI(Connector):
    def __init__(self, host: str, api_key: str, nickname: str, cohort: str) -> None:
        super().__init__(host, api_key, nickname, cohort)
        self.url = urljoin(self.host, 'deliveries')
    
