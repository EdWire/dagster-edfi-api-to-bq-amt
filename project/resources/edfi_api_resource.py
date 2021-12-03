from typing import List, Dict

import base64
import math
import requests

from dagster import get_dagster_logger, resource
from tenacity import retry, wait_exponential


class EdFiApiClient:
    '''Class for interacting with an Ed-Fi API'''

    def __init__(self, base_url, api_key, api_secret, school_year):
        self.base_url = base_url
        self.api_key = api_key
        self.api_secret = api_secret
        self.school_year = school_year
        self.limit = 100
        self.log = get_dagster_logger()
        self.access_token = self.get_access_token()

    def get_access_token(self):
        credentials_concatenated = ':'.join((self.api_key, self.api_secret))
        credentials_encoded = base64.b64encode(credentials_concatenated.encode('utf-8'))
        access_url = f'{self.base_url}/oauth/token'

        access_headers = {
            'Authorization': b'Basic ' + credentials_encoded
        }

        access_params = {
            'grant_type': 'client_credentials'
        }

        response = requests.post(access_url, headers=access_headers, data=access_params)

        if response.ok:
            response_json = response.json()
            access_token = response_json['access_token']
            self.log.debug(f'Retrieved access token {access_token}')
            return access_token
        else:
            raise Exception("Failed to retrieve access token")

    @retry(wait=wait_exponential(multiplier=1, min=4, max=10))
    def _call_api(self, url, headers):
        try:
            response = requests.get(url, headers=headers)
            response.raise_for_status()
        except requests.exceptions.HTTPError as err:
            self.log.warn(f'Failed to retrieve data: {err}')
            raise err
        
        return response.json()


    def get_data(self, api_endpoint) -> List[Dict]:
        headers = {'Authorization': f'Bearer {self.access_token}'}

        # determine if URL should include school year
        if self.school_year > 1901:
            endpoint = f'{self.base_url}/data/v3/{self.school_year}{api_endpoint}?limit={self.limit}&totalCount=true'
        else:
            endpoint = f'{self.base_url}/data/v3{api_endpoint}?limit={self.limit}&totalCount=true'

        result = list()
        try:
            # GET with no offset to determine total record count
            self.log.debug(f'Calling endpoint {endpoint}')
            response = requests.get(endpoint, headers=headers)
            response.raise_for_status()
            result = result + response.json()
        except requests.exceptions.HTTPError as err:
            self.log.warn(f'Failed to retrieve data: {err}')
            raise err

        number_times_to_run = math.ceil(int(response.headers['Total-Count']) / 100)
        for run_number in range(1, number_times_to_run):
            endpoint_to_call = f'{endpoint}&offset={run_number * self.limit}'
            self.log.debug(endpoint_to_call)
            response = self._call_api(endpoint_to_call, headers)
            result = result + response

        return result



@resource(
    config_schema={
        "base_url": str,
        "api_key": str,
        "api_secret": str,
        "school_year": int
    },
    description="Ed-Fi API client that retrieves data from various endpoints.",
)
def edfi_api_resource_client(context):
    return EdFiApiClient(
        context.resource_config["base_url"],
        context.resource_config["api_key"],
        context.resource_config["api_secret"],
        context.resource_config["school_year"]
    )
