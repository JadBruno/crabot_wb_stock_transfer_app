import requests
import ijson
from typing import Any, Dict, Optional, Union
from http import HTTPStatus
from utils.logger import get_logger

class APIRequestError(Exception):
    def __init__(self, message: str, status_code: Optional[int] = None):
        super().__init__(message)
        self.status_code = status_code


class SyncAPIController:
    _ALLOWED_METHODS = {"GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"}

    def __init__(self, timeout: int = 10):
        self.timeout = timeout
        self.logger = get_logger("SyncAPIController")

    def request(self,
                base_url: str,
                method: str,
                endpoint: str,
                *,
                params: Optional[Dict[str, Any]] = None,
                json: Optional[Dict[str, Any]] = None,
                data: Optional[Union[Dict[str, Any], str]] = None,
                headers: Optional[Dict[str, str]] = None,
                cookies: Optional[Dict[str, str]] = None,
                files: Optional[Dict[str, Any]] = None,
                auth: Optional[Any] = None,
                stream: bool = False,
                stream_path: Optional[str] = None,
                **kwargs) -> Any:
    
        base_url = base_url.rstrip("/")

        method = method.upper()
        if method not in self._ALLOWED_METHODS:
            raise ValueError(f"Unsupported HTTP method: {method}")

        url = f"{base_url}/{endpoint.lstrip('/')}"

        filtered_kwargs = {"params": params or None,
                            "json": json or None,
                            "data": data or None,
                            "headers": headers or None,
                            "cookies": cookies or None,
                            "files": files or None,
                            "auth": auth or None,
                            "timeout": self.timeout,
                            "stream": stream,
                            **kwargs,
                            }
        
        hidden_arg_keys = ['headers', 'cookies', 'auth']
        request_args = {k: v for k, v in filtered_kwargs.items() if v is not None}
        request_args_for_logs = {k: v for k, v in filtered_kwargs.items() if k not in hidden_arg_keys and v is not None}


        try:
            self.logger.debug(f"{method} Request to {url} | args={request_args_for_logs}")
            response = requests.request(method=method, url=url, **request_args)
            return response

        except requests.exceptions.HTTPError as http_err:
            return self._handle_http_error(http_err.response)

        except requests.exceptions.RequestException as req_err:
            self.logger.error(f"Request failed: {req_err}")
            return {"status_code": 503,
                    "error": "Service Unavailable",
                    "details": {"message": str(req_err)}}

        except Exception as e:
            self.logger.exception("Unexpected error occurred")
            return {"status": 500,
                    "error": "Internal Server Error",
                    "details": {"message": str(e)}}
        
    def _parse_response(self, response: requests.Response, stream: bool, stream_path: Optional[str]) -> Any:
        content_type = response.headers.get("Content-Type", "")
        if "application/json" not in content_type:
            return response.text

        if stream:
            # Ensure raw is ready for binary reading
            response.encoding = "utf-8"
            try:
                if stream_path:
                    result = list(ijson.items(response.raw, stream_path))
                else:
                    result = list(ijson.items(response.raw, "item"))
                self.logger.info(f"Streamed JSON parsed: {len(result)} items")
                return result
            finally:
                response.close()
        else:
            parsed = response.json()

            if isinstance(parsed, dict):
                summary = f"first five keys={list(parsed.keys())[:5]}"
            elif isinstance(parsed, list):
                summary = f"list of {len(parsed)} items"
            else:
                summary = f"type={type(parsed).__name__}"

            self.logger.info(f"Response from {response.url} | Status: {response.status_code} | JSON: {summary}")
            return parsed


    def _handle_http_error(self, response: requests.Response) -> dict:
        try:
            error_body = response.json()
        except Exception:
            error_body = {"message": response.text or "<no content>"}

        status_code = response.status_code
        self.logger.warning(f"HTTP Error {status_code}: {error_body}")

        error_response = {
            "status": status_code,
            "error": HTTPStatus(status_code).phrase if status_code in HTTPStatus.__members__.values() else "HTTP Error",
            "details": error_body}

        return error_response