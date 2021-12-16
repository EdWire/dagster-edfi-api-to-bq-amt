from typing import List, Dict

import base64
import requests

from dagster import get_dagster_logger, resource
from tenacity import retry, wait_exponential


class EdFiApiClient:
    """Class for interacting with an Ed-Fi API"""

    def __init__(self, base_url, api_key, api_secret, school_year):
        self.base_url = base_url
        self.api_key = api_key
        self.api_secret = api_secret
        self.school_year = school_year
        self.log = get_dagster_logger()
        self.access_token = self.get_access_token()


    def get_access_token(self):
        """
        Retrieve access token from Ed-Fi API.
        """
        credentials_concatenated = ":".join((self.api_key, self.api_secret))
        credentials_encoded = base64.b64encode(credentials_concatenated.encode('utf-8'))
        access_url = f"{self.base_url}/oauth/token"
        access_headers = {
            "Authorization": b"Basic " + credentials_encoded
        }
        access_params = { "grant_type": "client_credentials" }

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
        """
        Call GET on passed in URL and
        return response.
        """
        try:
            response = requests.get(url, headers=headers)
            response.raise_for_status()
        except requests.exceptions.HTTPError as err:
            self.log.warn(f'Failed to retrieve data: {err}')
            raise err

        return response.json()


    def get_available_change_versions(self) -> List[Dict]:
        headers = {'Authorization': f'Bearer {self.access_token}'}

        # determine if URL should include school year
        if self.school_year > 1901:
            endpoint = f'{self.base_url}/changeQueries/v1/{self.school_year}/availableChangeVersions'
        else:
            endpoint = f'{self.base_url}/changeQueries/v1/availableChangeVersions'
        
        return self._call_api(endpoint, headers)


    def get_data(self, api_endpoint: str,
        latest_processed_change_version: int, newest_change_version: int) -> List[Dict]:

        headers = {'Authorization': f'Bearer {self.access_token}'}

        if "/deletes" in api_endpoint:
            limit = 5000
        else:
            limit = 100

        # determine if URL should include school year
        if self.school_year > 1901:
            endpoint = (
                f"{self.base_url}/data/v3/{self.school_year}{api_endpoint}"
                f"?limit={limit}&minChangeVersion={latest_processed_change_version + 1}"
                f"&maxChangeVersion={newest_change_version}"
            )
        else:
            endpoint = (
                f"{self.base_url}/data/v3{api_endpoint}"
                f"?limit={limit}&minChangeVersion={latest_processed_change_version + 1}"
                f"&maxChangeVersion={newest_change_version}"
            )

        result = list()
        offset = 0
        while True:
            endpoint_to_call = f'{endpoint}&offset={offset}'
            self.log.debug(endpoint_to_call)
            response = self._call_api(endpoint_to_call, headers)
            result = result + response

            if not response:
                # retrieved all data from api
                break
            else:
                # move onto next page
                offset = offset + limit

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
