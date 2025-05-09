from abc import ABC, abstractmethod

class RequestAdapter(ABC):
    @abstractmethod
    def execute(self, request: "Request") -> "Response":
        pass

class AsyncRequestAdapter(ABC):
    @abstractmethod
    async def execute(self, request: "Request") -> "Response":
        pass

    @abstractmethod
    def set_engine(self, engine: "Engine"):
        pass

class DiscovererAdapter(ABC):
    @abstractmethod
    def discover(self, response: "Response") -> list[str]:
        pass

    @abstractmethod
    def set_engine(self, engine: "Engine"):
        pass
    