import asyncio
import collections
import datetime
import os
import re
from typing import Callable, Any, Deque, Dict, Optional, List, Set, Union

import aiohttp
import aiofiles

# Import Textual components
from textual.app import App, ComposeResult
from textual.widgets import Header, Footer, Tree, RichLog # Use RichLog for logs
from textual.containers import Container, VerticalScroll, HorizontalScroll # Use containers for layout
from textual.reactive import var # For reactive state if needed
from textual.message import Message # Import Message for custom messages

# Assuming these are in separate files as before
from .request import Request
from .response import Response
from .adapter import RequestAdapter, AsyncRequestAdapter, DiscovererAdapter
from .pipeline import Pipeline

# --- Existing Crawler Logic (Adapted) ---

class UrlWrapper:
    def __init__(self, url: str, depth: int, parent_url: Optional[str] = None):
        self.url = url
        self.depth = depth
        self.parent_url = parent_url

    def __repr__(self):
        return f"<UrlWrapper url='{self.url}' depth={self.depth} parent_url={self.parent_url}>"

# Define custom Textual messages for updating the UI from the Engine
class UpdateTreeNodeMessage(Message):
    """Message to update a node in the Textual Tree."""
    def __init__(self, url: str, label: str, parent_url: Optional[str] = None):
        self.url = url
        self.label = label
        self.parent_url = parent_url
        super().__init__()

class Engine:
    def __init__(self, request_adapter: AsyncRequestAdapter,
                 discoverer_adapter: DiscovererAdapter,
                 pipeline: Pipeline,
                 textual_app: "CrawlerApp", # Reference to the Textual App
                 max_workers: int = 10,
                 initial_max_depth: int = 1):
        self.request_adapter = request_adapter
        self.discoverer_adapter = discoverer_adapter
        self.pipeline = pipeline
        self.textual_app = textual_app # Store the app reference
        self.seen_urls: Set[str] = set()
        self.to_visit: Deque[UrlWrapper] = collections.deque()

        self._semaphore = asyncio.Semaphore(max_workers)
        self.max_depth = initial_max_depth
        self.active_tasks: Set[asyncio.Task] = set()
        # No need for self._loop here, Textual manages the asyncio loop

        # Pass engine instance to adapters that might need it for logging or context
        if hasattr(self.request_adapter, 'set_engine'):
             self.request_adapter.set_engine(self)
        if hasattr(self.discoverer_adapter, 'set_engine'):
             self.discoverer_adapter.set_engine(self)

    def logger(self, message: str, style: Optional[str] = None):
        """Sends a log message to the Textual app's log widget."""
        # Use call_from_thread if this method could be called from a non-Textual thread
        # Since our workers are asyncio tasks within the same loop as Textual,
        # direct calls to widget methods should be safe.
        timestamp = datetime.datetime.now().strftime("[%X]")
        # Apply style directly in the log message string using Rich tags
        styled_message = f"{message}[/]"
        log_text = f"[{style or 'white'}]{timestamp} {styled_message}"
        self.textual_app.query_one(RichLog).write(log_text) # Write to the RichLog widget

    # Removed _update_tree from Engine, UI updates will be handled by Textual App
    # via messages

    async def _worker(self, url_wrapper: UrlWrapper):
        """Fetches, processes, and discovers links for a single URL."""
        url = url_wrapper.url
        depth = url_wrapper.depth
        parent_url = url_wrapper.parent_url

        if url in self.seen_urls:
            # Post message to update tree node as skipped
            self.textual_app.post_message(UpdateTreeNodeMessage(url, f"[dim]{url} (depth {depth}) - Skipped[/]", parent_url))
            return
        self.seen_urls.add(url)

        # Post message to update tree node as visiting
        self.textual_app.post_message(UpdateTreeNodeMessage(url, f"[yellow]{url} (depth {depth}) - Visiting[/]", parent_url))

        self.logger(f"[{depth}] Visiting: {url}", style="yellow")

        request = Request(depth=depth, url=url)

        try:
            response = await self.request_adapter.execute(request)
            # Post message to update tree node as fetched/processing
            self.textual_app.post_message(UpdateTreeNodeMessage(url, f"[cyan]{url} (depth {depth}) - Fetched[/]", parent_url))

            self.logger(f"[{depth}] Fetched: {url} with status {response.status_code}", style="green")
            await self._process_response(response)

            # Post message to update tree node as processed
            self.textual_app.post_message(UpdateTreeNodeMessage(url, f"[green]{url} (depth {depth}) - Processed[/]", parent_url))

        except aiohttp.ClientError as e:
            # Post message to update tree node as error
            self.textual_app.post_message(UpdateTreeNodeMessage(url, f"[red]{url} (depth {depth}) - Network Error[/]", parent_url))
            self.logger(f"[{depth}] Network error fetching {url}: {e}", style="bold red")
        except Exception as e:
            # Post message to update tree node as error
            self.textual_app.post_message(UpdateTreeNodeMessage(url, f"[red]{url} (depth {depth}) - Processing Error[/]", parent_url))
            self.logger(f"[{depth}] Error processing {url}: {e}", style="bold red")

    async def _process_response(self, response: Response):
        self.logger(f"Processing content from {response.request.url} (status: {response.status_code})")
        try:
            res = await self.pipeline.process(response)
            self.logger(f"Finished processing content from {response.request.url} (result: {res})", style="bold green")
        except Exception as e:
             self.logger(f"Error during pipeline processing for {response.request.url}: {e}", style="bold red")
             # The worker's exception handling will update the tree node to error


        if response.request.depth < self.max_depth:
            try:
                new_urls = self._discover(response)
                for new_url in new_urls:
                    if new_url not in self.seen_urls:
                        # Add to to_visit with the current URL as the parent
                        self.to_visit.append(UrlWrapper(new_url, response.request.depth + 1, parent_url=response.request.url))
                        # Post message to add the new URL to the tree (initially unvisited/queued)
                        self.textual_app.post_message(UpdateTreeNodeMessage(new_url, f"{new_url} (depth {response.request.depth + 1})", response.request.url))
                        self.logger(f"[{response.request.depth + 1}] Discovered: {new_url} from {response.request.url}")
            except Exception as e:
                 self.logger(f"Error during URL discovery for {response.request.url}: {e}", style="bold red")
        else:
            self.logger(f"[{response.request.depth}] Max depth reached for links from {response.request.url}", style="dim")

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
        self.logger(f"Starting crawl up to depth {self.max_depth}", style="bold")

        # Add the initial URL to the tree immediately when dispatch starts
        if self.to_visit: # Should contain the initial URL
             initial_wrapper = self.to_visit[0] # Assuming initial URL is the first added
             self.textual_app.post_message(UpdateTreeNodeMessage(initial_wrapper.url, f"{initial_wrapper.url} (depth {initial_wrapper.depth})", initial_wrapper.parent_url))


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
                        self.logger("Task was cancelled.", style="yellow")
                        # No specific tree update for cancellation in this scheme,
                        # the node might remain in its last known state or error state
                    except Exception as e:
                        self.logger(f"Error retrieving task exception: {e}", style="red")

                task.add_done_callback(task_done_callback)

            if not self.to_visit and self.active_tasks:
                 await asyncio.wait(self.active_tasks, return_when=asyncio.FIRST_COMPLETED)
            else:
                 await asyncio.sleep(0.05)

        if self.active_tasks:
            self.logger(f"Waiting for {len(self.active_tasks)} remaining tasks to finish...", style="yellow")
            await asyncio.gather(*self.active_tasks, return_exceptions=True)

        self.logger("Crawler finished.", style="bold")
        # Optionally quit the app or show a final message
        # self.textual_app.exit()
