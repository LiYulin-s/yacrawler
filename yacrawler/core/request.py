from typing import Any, Dict, Optional


class Request:
    def __init__(self, url: str, depth: int, method: str = "GET", headers: Optional[Dict[str, str]] = None, data: Any = None):
        self.url = url
        self.depth = depth
        self.method = method
        self.headers = headers or {}
        self.data = data

    def __repr__(self):
        return f"<Request url='{self.url}' depth={self.depth}>"