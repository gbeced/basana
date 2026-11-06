# Basana
#
# Copyright 2026 Gabriel Martin Becedillas Ruiz
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
A minimal HTTP mock built on top of aiohttp's first-party test server.

Unlike aioresponses, this doesn't monkey-patch aiohttp internals. A real local HTTP server is started and the code
under test is pointed to it using :attr:`HTTPMock.base_url`. Responses are registered using an aioresponses like API,
and URLs are matched against a virtual URL that uses the host supplied in the constructor, so that tests can keep
registering URLs like `http://binance.mock/api/v3/depth`.
"""

from typing import Any, Awaitable, Callable, Dict, List, Optional, Pattern, Tuple, Union
import asyncio
import dataclasses
import inspect
import json

from aiohttp import test_utils, web
from multidict import CIMultiDictProxy, MultiMapping
from yarl import URL


@dataclasses.dataclass
class CallbackResult:
    status: int = 200
    payload: Any = None
    body: Optional[Union[str, bytes]] = None
    headers: Optional[Dict[str, str]] = None


@dataclasses.dataclass
class RequestCall:
    headers: CIMultiDictProxy[str]
    params: MultiMapping[str]
    data: Optional[Dict[str, Any]]
    json: Any


Callback = Callable[..., Union[CallbackResult, Awaitable[CallbackResult]]]


def _normalize_url(url: Union[str, URL]) -> URL:
    url = URL(url)
    return url.with_query(sorted(url.query.items()))


@dataclasses.dataclass
class _Matcher:
    method: str
    url: Union[URL, Pattern[str]]
    response: CallbackResult
    callback: Optional[Callback]
    repeat: bool

    def matches(self, method: str, url: URL) -> bool:
        if method != self.method:
            return False
        if isinstance(self.url, URL):
            return self.url == url
        return self.url.match(str(url)) is not None


class HTTPMock:
    def __init__(self, host: str):
        self._host = host
        self._server = test_utils.RawTestServer(self._handle)
        self._matchers: List[_Matcher] = []
        self.requests: Dict[Tuple[str, URL], List[RequestCall]] = {}

    async def __aenter__(self) -> "HTTPMock":
        await self._server.start_server()
        return self

    async def __aexit__(self, *args: Any) -> None:
        await self._server.close()

    @property
    def base_url(self) -> str:
        return str(self._server.make_url("/"))

    def add(
            self, url: Union[str, Pattern[str]], method: str = "GET", status: int = 200, payload: Any = None,
            body: Optional[Union[str, bytes]] = None, headers: Optional[Dict[str, str]] = None,
            callback: Optional[Callback] = None, repeat: bool = False
    ):
        self._matchers.append(_Matcher(
            method=method.upper(),
            url=_normalize_url(url) if isinstance(url, str) else url,
            response=CallbackResult(status=status, payload=payload, body=body, headers=headers),
            callback=callback,
            repeat=repeat,
        ))

    def get(self, url: Union[str, Pattern[str]], **kwargs: Any):
        self.add(url, method="GET", **kwargs)

    def post(self, url: Union[str, Pattern[str]], **kwargs: Any):
        self.add(url, method="POST", **kwargs)

    def put(self, url: Union[str, Pattern[str]], **kwargs: Any):
        self.add(url, method="PUT", **kwargs)

    def delete(self, url: Union[str, Pattern[str]], **kwargs: Any):
        self.add(url, method="DELETE", **kwargs)

    def clear(self):
        self._matchers.clear()
        self.requests.clear()

    async def _handle(self, request: web.BaseRequest) -> web.StreamResponse:
        url = _normalize_url(f"http://{self._host}{request.path_qs}")
        data = None
        json_body = None
        if request.content_type in ("application/x-www-form-urlencoded", "multipart/form-data"):
            data = dict(await request.post())
        elif request.content_type == "application/json":
            json_body = await request.json()
        call = RequestCall(headers=request.headers, params=request.query, data=data, json=json_body)
        self.requests.setdefault((request.method, url), []).append(call)

        matcher = next((m for m in self._matchers if m.matches(request.method, url)), None)
        if matcher is None:
            # Mimic a connection error, like aioresponses does for unmatched requests.
            assert request.transport is not None
            request.transport.close()
            raise asyncio.CancelledError()
        if not matcher.repeat:
            self._matchers.remove(matcher)

        response = matcher.response
        if matcher.callback:
            result = matcher.callback(url, headers=call.headers, params=call.params, data=data, json=json_body)
            response = await result if inspect.isawaitable(result) else result

        if response.payload is not None:
            body: Union[str, bytes] = json.dumps(response.payload)
            content_type = "application/json"
        else:
            body = response.body or b""
            content_type = "text/plain"
        if isinstance(body, str):
            body = body.encode()
        ret = web.Response(status=response.status, body=body, content_type=content_type)
        if response.headers:
            ret.headers.update(response.headers)
        return ret
