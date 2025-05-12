import asyncio

from rich.console import Console

from yacrawler.core import *

from yacrawler.cli.cli_logger import CliLogger

class CrawlerCliApp:
    def __init__(self, start_urls: list[str], max_depth: int, max_workers: int, request_adapter: AsyncRequestAdapter,
                 discoverer_adapter: DiscovererAdapter, pipeline: Pipeline):
        self.start_urls = start_urls
        self.max_depth = max_depth
        self.max_workers = max_workers
        self.request_adapter = request_adapter
        self.discoverer_adapter = discoverer_adapter
        self.pipeline = pipeline
        self.console = Console()
        self.logger_adapter = CliLogger(self.console)
        self.engine = Engine(request_adapter, discoverer_adapter, pipeline, self.logger_adapter, max_workers=self.max_workers,
                            initial_max_depth=self.max_depth)

    def run(self):
        self.engine.set_start_urls(self.start_urls)
        asyncio.run(self.engine.dispatch())


