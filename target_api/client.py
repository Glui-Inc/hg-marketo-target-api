from __future__ import annotations

import os

from pydantic import BaseModel
from target_hotglue.auth import ApiAuthenticator
from target_hotglue.client import HotglueBaseSink
import requests
from singer_sdk.exceptions import FatalAPIError, RetriableAPIError
from curlify import to_curl

from target_api.auth import MarketoApiKeyAuthenticator, MarketoAuthenticator


class ApiSink(HotglueBaseSink):
    @property
    def name(self):
        return self.stream_name
    

    auth_state = {}

    @property
    def authenticator(self):
        self.logger.info("+++++++++++++++++ CONFIG URL in AUTHENTICATOR")
        self.logger.info(self._config["url"])
        # auth with hapikey
        if self.config.get("api_private_key"):
            api_key = self.config.get("api_private_key")
            return MarketoApiKeyAuthenticator(self._target, api_key)
        # auth with acces token
        # url = "https://a.klaviyo.com/oauth/token"
        client_id = os.environ.get("client_id")
        secret_id = os.environ.get("secret_id")
        # url = "GET <Identity URL>/oauth/token?grant_type=client_credentials&client_id=<Client Id>&client_secret=<Client Secret>"
        url = f"https://api.playrcart.com/oauth-token?test=apitest&grant_type=client_credentials&client_id={client_id}&client_secret={secret_id}"
        return MarketoAuthenticator(self._target, self.auth_state, url)
    
    # @property
    # def authenticator(self):
    #     return (
    #         ApiAuthenticator(
    #             self._target,
    #             header_name=os.environ.get("CUSTOM_INTEGRATION_DEFAULT_API_KEY", "x-api-key"),
    #         )
    #         if self._config.get("auth", False) or self._config.get("api_key_url")
    #         else None
    #     )

    @property
    def base_url(self) -> str:
        tenant_id = os.environ.get("TENANT")
        flow_id = os.environ.get("FLOW")
        tap = os.environ.get("TAP", None)
        connector_id = os.environ.get("CONNECTOR_ID", None)

        self.logger.info("+++++++++++++++++ CONFIG URL in BASE URL")
        self.logger.info(self._config["url"])
        base_url = self._config["url"].format(
            stream=self.stream_name,
            tenant=tenant_id,
            tenant_id=tenant_id,
            flow=flow_id,
            flow_id=flow_id,
            tap=tap,
            connector_id=connector_id,
        )

        if self._config.get("api_key_url"):
            base_url += (
                f"?{os.environ.get('CUSTOM_INTEGRATION_DEFAULT_API_KEY', 'x-api-key')}={self._config.get('api_key')}"
            )

        return base_url

    @property
    def endpoint(self) -> str:
        return ""

    @property
    def unified_schema(self) -> BaseModel:
        return None

    @property
    def custom_headers(self) -> dict:
        custom_headers = {
            "User-Agent": self._config.get("user_agent", "target-api <hello@hotglue.xyz>")
        }
        config_custom_headers = self._config.get("custom_headers") or list()
        for ch in config_custom_headers:
            if not isinstance(ch, dict):
                continue
            name = ch.get("name")
            value = ch.get("value")
            if not isinstance(name, str) or not isinstance(value, str):
                continue
            custom_headers[name] = value
        return custom_headers
        
    def response_error_message(self, response: requests.Response) -> str:
        try:
            response_text = f" with response body: '{response.text}'"
        except:
            response_text = None
        return f"Status code: {response.status_code} with {response.reason} for path: {response.request.url} {response_text}"
    
    def curlify_on_error(self, response):
        curl = to_curl(response.request)
        return curl

    def validate_response(self, response: requests.Response) -> None:
        """Validate HTTP response."""
        if response.status_code in [429] or 500 <= response.status_code < 600:
            msg = self.response_error_message(response)
            curl = self.curlify_on_error(response)
            self.logger.info(f"cURL: {curl}")
            error = {"status_code": response.status_code, "body": msg}
            raise RetriableAPIError(error)
        elif 400 <= response.status_code < 500:
            msg = self.response_error_message(response)
            curl = self.curlify_on_error(response)
            self.logger.info(f"cURL: {curl}")
            error = {"status_code": response.status_code, "body": msg}
            raise FatalAPIError(error)
