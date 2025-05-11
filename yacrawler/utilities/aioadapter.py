from typing import Optional

import aiohttp

from yacrawler.core.adapter import AsyncRequestAdapter
from yacrawler.core.engine import Engine
from yacrawler.core.request import Request
from yacrawler.core.response import Response


class AioRequest(AsyncRequestAdapter):
    async def execute(self, request: Request) -> Response:
        timeout = aiohttp.ClientTimeout(total=30)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            if self.engine:
                 self.engine.log_adapter.log(f"Fetching {request.url} with depth {request.depth}", level="INFO")
            else:
                 print(f"Fetching {request.url} with depth {request.depth}")

            try:
                async with session.get(request.url, allow_redirects=True) as response:
                    body = await response.read()
                    headers = {str(k): str(v) for k, v in response.headers.items()}
                    return Response(request=request, body=body, status_code=response.status, headers=headers)
            except aiohttp.ClientError as e:
                 if self.engine:
                     self.engine.log_adapter.log(f"Aiohttp error fetching {request.url}: {e}", level="ERROR")
                 else:
                     print(f"Aiohttp error fetching {request.url}: {e}")
                 raise

    def __init__(self):
        self.engine: Optional[Engine] = None

    def set_engine(self, engine: Engine):
        self.engine = engine