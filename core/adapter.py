from abc import ABC, abstractmethod

from request import Request
from response import Response

class RequestAdapter(ABC):
    @abstractmethod
    def execute(self, request: Request) -> Response:
        pass

class AsyncRequestAdapter(ABC):
    @abstractmethod
    async def execute(self, request: Request) -> Response:
        pass

class DiscovererAdapter(ABC):
    @abstractmethod
    def discover(self, response: Response) -> list[str]:
        pass
    