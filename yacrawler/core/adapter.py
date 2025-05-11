from abc import ABC, abstractmethod

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
    
class LoggerAdapter(ABC):
    @abstractmethod
    def log(self, message: str, level: str):
        pass

    @abstractmethod
    def update_node(self, url: str, label: str, status: str, parent_url: str):
        pass