import asyncio
import collections
from typing import Deque, Optional, Set

import aiohttp

from .adapter import AsyncRequestAdapter, DiscovererAdapter, LoggerAdapter
from .pipeline import Pipeline
# Assuming these are in separate files as before
from .request import Request
from .response import Response

class UrlWrapper:
    def __init__(self, url: str, depth: int, parent_url: Optional[str] = None):
        self.url = url
        self.depth = depth
        self.parent_url = parent_url

    def __repr__(self):
        return f"<UrlWrapper url='{self.url}' depth={self.depth} parent_url={self.parent_url}>"


class Engine:
    def __init__(self, request_adapter: AsyncRequestAdapter,
                 discoverer_adapter: DiscovererAdapter,
                 pipeline: Pipeline,
                 log_adapter: LoggerAdapter,
                 max_workers: int = 10,
                 initial_max_depth: int = 1):
        self.request_adapter = request_adapter
        self.discoverer_adapter = discoverer_adapter
        self.log_adapter = log_adapter
        self.pipeline = pipeline
        # self.textual_app = textual_app # Store the app reference
        self.seen_urls: Set[str] = set()
        self.to_visit: Deque[UrlWrapper] = collections.deque()

        self._semaphore = asyncio.Semaphore(max_workers)
        self.max_depth = initial_max_depth
        self.active_tasks: Set[asyncio.Task] = set()

        if hasattr(self.request_adapter, 'set_engine'):
            self.request_adapter.set_engine(self)
        if hasattr(self.discoverer_adapter, 'set_engine'):
            self.discoverer_adapter.set_engine(self)

    async def _worker(self, url_wrapper: UrlWrapper):
        """Fetches, processes, and discovers links for a single URL."""
        url = url_wrapper.url
        depth = url_wrapper.depth
        parent_url = url_wrapper.parent_url

        if url in self.seen_urls:
            self.log_adapter.update_node(url, f"{url} (depth {depth})", "SKIPPED", parent_url)
            return
        self.seen_urls.add(url)

        # Post message to update tree node as visiting
        self.log_adapter.update_node(url, f"{url} (depth {depth})", "VISITING", parent_url)
        self.log_adapter.log(f"[{depth}] Visiting: {url}", level="INFO")

        request = Request(depth=depth, url=url)

        try:
            response = await self.request_adapter.execute(request)
            self.log_adapter.update_node(url, f"{url} (depth {depth})", "PROCESSING", parent_url)

            self.log_adapter.log(f"[{depth}] Fetched: {url} with status {response.status_code}", level="INFO")
            await self._process_response(response)

            self.log_adapter.update_node(url, f"{url} (depth {depth})", "PROCESSED", parent_url)

        except aiohttp.ClientError as e:
            # Post message to update tree node as error
            self.log_adapter.update_node(url, f"{url} (depth {depth})", "ERROR", parent_url)
            self.log_adapter.log(f"[{depth}] Network error fetching {url}: {e}", level="ERROR")
        except Exception as e:
            # Post message to update tree node as error
            self.log_adapter.update_node(url, f"{url} (depth {depth})", "ERROR", parent_url)
            self.log_adapter.log(f"[{depth}] Error processing {url}: {e}", level="ERROR")

    async def _process_response(self, response: Response):
        self.log_adapter.log(f"Processing content from {response.request.url} (status: {response.status_code})",
                             level="INFO")
        try:
            res = await self.pipeline.process(response)
            self.log_adapter.log(f"Finished processing content from {response.request.url} (result: {res})",
                                 level="INFO")
        except Exception as e:
            self.log_adapter.log(f"Error during pipeline processing for {response.request.url}: {e}", level="ERROR")

        if response.request.depth < self.max_depth:
            try:
                new_urls = self._discover(response)
                for new_url in new_urls:
                    if new_url not in self.seen_urls:
                        self.to_visit.append(
                            UrlWrapper(new_url, response.request.depth + 1, parent_url=response.request.url))
                        self.log_adapter.update_node(new_url, f"{new_url} (depth {response.request.depth + 1})",
                                                     "PENDING", response.request.url)
                        self.log_adapter.log(
                            f"[{response.request.depth + 1}] Discovered: {new_url} from {response.request.url}",
                            level="INFO")
            except Exception as e:
                self.log_adapter.log(f"Error during URL discovery for {response.request.url}: {e}", level="ERROR")
        else:
            self.log_adapter.log(f"[{response.request.depth}] Max depth reached for links from {response.request.url}",
                                 level="INFO")

    def _discover(self, response: Response) -> list[str]:
        urls = self.discoverer_adapter.discover(response)
        valid_urls = []
        for url in urls:
            if url and url.startswith("http"):
                parsed_url = url.split('#')[0]
                valid_urls.append(parsed_url)
        return valid_urls

    async def dispatch(self):
        """Asynchronous dispatch loop, run as a Textual worker."""
        self.log_adapter.log(f"Starting crawl up to depth {self.max_depth}", level="WARNING")

        if self.to_visit:  # Should contain the initial URL
            initial_wrapper = self.to_visit[0]  # Assuming initial URL is the first added
            self.log_adapter.update_node(initial_wrapper.url, f"{initial_wrapper.url} (depth {initial_wrapper.depth})",
                                         "PENDING", initial_wrapper.parent_url)

        while self.active_tasks or self.to_visit:
            while self.to_visit and not self._semaphore.locked():
                await self._semaphore.acquire()

                url_wrapper = self.to_visit.popleft()

                if url_wrapper.url in self.seen_urls:
                    self._semaphore.release()
                    # The skipping message is now handled in _worker
                    continue

                # Create the worker task
                task = asyncio.create_task(self._worker(url_wrapper))
                self.active_tasks.add(task)

                # Add a done callback to release the semaphore and remove the task
                def task_done_callback(t):
                    self._semaphore.release()
                    if t in self.active_tasks:
                        self.active_tasks.remove(t)
                    try:
                        exception = t.exception()
                        if exception:
                            # Error logging and tree update is handled in _worker
                            pass
                    except asyncio.CancelledError:
                        self.log_adapter.log("Task was cancelled.", level="WARNING")
                        # No specific tree update for cancellation in this scheme,
                        # the node might remain in its last known state or error state
                    except Exception as e:
                        self.log_adapter.log(f"Error retrieving task exception: {e}", level="ERROR")

                task.add_done_callback(task_done_callback)

            if not self.to_visit and self.active_tasks:
                await asyncio.wait(self.active_tasks, return_when=asyncio.FIRST_COMPLETED)
            else:
                await asyncio.sleep(0.05)

        if self.active_tasks:
            self.log_adapter.log(f"Waiting for {len(self.active_tasks)} remaining tasks to finish...", level="WARNING")
            await asyncio.gather(*self.active_tasks, return_exceptions=True)

        self.log_adapter.log("Crawler finished.", level="WARNING")
