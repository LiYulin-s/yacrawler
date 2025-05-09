from typing import Dict
from .request import Request


class Response:
    def __init__(self, request: Request, status_code: int, headers: Dict[str, str], body: bytes):
        self.request = request
        self.status_code = status_code
        self.headers = headers
        self.body = body

    def __repr__(self):
        return f"<Response url='{self.request.url}' status={self.status_code}>"