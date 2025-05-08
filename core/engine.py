import asyncio
import concurrent.futures as coc # Not actively used, but in original imports
import collections
import re
from typing import Callable, Any, Deque, Dict, Optional, List, Set # Added Set

import aiohttp
import aiofiles


from request import Request
from response import Response

from adapter import RequestAdapter, AsyncRequestAdapter, DiscovererAdapter 

from pipeline import Pipeline

from rich.console import Console # Kept as it's initialized


class UrlWrapper:
    def __init__(self, url: str, depth: int):
        self.url = url
        self.depth = depth

    def __repr__(self):
        return f"<UrlWrapper url='{self.url}' depth={self.depth}>"

class Engine:
    def __init__(self, request_adapter: AsyncRequestAdapter, # Specify AsyncRequestAdapter
                 discoverer_adapter: DiscovererAdapter,
                 pipeline: Pipeline,
                 max_workers: int = 10,
                 initial_max_depth: int = 1): # Renamed max_depth to initial_max_depth for clarity
        self.request_adapter = request_adapter
        self.discoverer_adapter = discoverer_adapter
        self.pipeline = pipeline
        self.seen_urls: Set[str] = set()
        self.to_visit: Deque[UrlWrapper] = collections.deque()
        self.console = Console() # Initialized, can be used for logging
        self._semaphore = asyncio.Semaphore(max_workers)
        self.max_depth = initial_max_depth # This is the max depth for crawling
        self._loop = asyncio.get_event_loop() # Better to get loop when needed or pass it
        self.active_tasks: Set[asyncio.Task] = set()

    async def _worker(self, url_wrapper: UrlWrapper):
        """Fetches, processes, and discovers links for a single URL."""
        url = url_wrapper.url
        depth = url_wrapper.depth

        if url in self.seen_urls:
            return
        self.seen_urls.add(url)

        self.console.log(f"[{depth}] Visiting: {url}")
        request = Request(depth=depth, url=url)

        try:
            response = await self.request_adapter.execute(request)
            self.console.log(f"[{depth}] Fetched: {url} with status {response.status_code}")
            await self._process_response(response)
        except aiohttp.ClientError as e: # More specific error handling
            self.console.log(f"[{depth}] Network error fetching {url}: {e}", style="bold red")
        except Exception as e:
            self.console.log(f"[{depth}] Error processing {url}: {e}", style="bold red")
            # Optionally re-raise or handle more gracefully

    async def _process_response(self, response: Response):
        # Process the response content itself (e.g., extract data)
        # Example: use the pipeline
        self.console.log(f"Processing content from {response.request.url} (status: {response.status_code})")
        res = await self.pipeline.process(response)
        self.console.log(f"Finished processing content from {response.request.url} (result: {res})", style="bold green")

        # Discover new URLs if depth allows
        if response.request.depth < self.max_depth:
            new_urls = self._discover(response)
            for new_url in new_urls:
                # Check again if seen, as multiple pages might link to the same new URL
                # and it might have been added to to_visit but not yet processed by a worker
                if new_url not in self.seen_urls: # Check self.seen_urls, not just existence in to_visit
                    # Add to seen_urls here to prevent adding to queue multiple times
                    # before a worker picks it up. Or check before adding to queue.
                    # self.seen_urls.add(new_url) # Add when processing starts or before queueing
                    self.to_visit.append(UrlWrapper(new_url, response.request.depth + 1))
                    # Log discovery, not processing yet
                    self.console.log(f"[{response.request.depth + 1}] Discovered: {new_url} from {response.request.url}")
        else:
            self.console.log(f"[{response.request.depth}] Max depth reached for links from {response.request.url}")


    def _discover(self, response: Response) -> list[str]:
        urls = self.discoverer_adapter.discover(response)
        valid_urls = []
        for url in urls:
            # Basic validation and normalization could happen here
            if url.startswith("http"): # Keep original simple filter
                # Add more normalization if needed (e.g. removing fragments)
                valid_urls.append(url)
        return valid_urls

   

    async def dispatch(self):
        while self.active_tasks or self.to_visit:
            while self.to_visit and not self._semaphore.locked(): # Check if semaphore has capacity
                await self._semaphore.acquire() # Acquire before creating task
                
                url_wrapper = self.to_visit.popleft()
                
                # Double check if URL has been seen, in case it was added
                # to to_visit multiple times before seen_urls was updated by a worker.
                # More robust seen check is at the start of _worker.
                if url_wrapper.url in self.seen_urls and not (len(self.seen_urls) == 1 and url_wrapper.depth == 0) : # Allow initial URL
                     self._semaphore.release() # Release if skipping
                     continue

                task = self._loop.create_task(self._worker(url_wrapper))
                self.active_tasks.add(task)
                
                # Callback to release semaphore and remove task from active set
                task.add_done_callback(self._task_done_callback)
            
            await asyncio.sleep(0.1) # Wait for tasks to progress or new items

        # Optional: Wait for any straggling tasks if logic allows (should be covered by while active_tasks)
        if self.active_tasks:
            await asyncio.gather(*self.active_tasks, return_exceptions=True) # Ensure all done

    def _task_done_callback(self, task: asyncio.Task):
        self._semaphore.release()
        self.active_tasks.remove(task)
        try:
            task.result() # To raise exceptions if any occurred in the task and weren't handled
        except Exception as e:
            self.console.log(f"Task completed with error: {e}", style="bold red")


    def run(self, start_url: str, max_depth_override: Optional[int] = None):
        """
        Starts the crawling process.
        start_url: The initial URL to crawl.
        max_depth_override: If provided, overrides the engine's configured max_depth for this run.
                           A typical start depth is 0.
        """
        if max_depth_override is not None:
            self.max_depth = max_depth_override
        
        # Initial URL is at depth 0
        initial_wrapper = UrlWrapper(start_url, 0)
        self.to_visit.append(initial_wrapper)
        # self.seen_urls.add(start_url) # Add start_url to seen_urls before starting dispatch

        self.console.log(f"Starting crawl from {start_url} up to depth {self.max_depth}")
        
        try:
            self._loop.run_until_complete(self.dispatch())
        except KeyboardInterrupt:
            self.console.log("Crawler interrupted by user.", style="yellow")
        finally:
            # Cleanup: Cancel any remaining tasks
            # This is important if run_until_complete is exited prematurely
            # (e.g. by KeyboardInterrupt if not caught inside dispatch, or other exceptions)
            remaining_tasks = [t for t in self.active_tasks if not t.done()]
            if remaining_tasks:
                self.console.log(f"Cancelling {len(remaining_tasks)} outstanding tasks...", style="yellow")
                for t in remaining_tasks:
                    t.cancel()
                # Allow tasks to process cancellation
                # self._loop.run_until_complete(asyncio.gather(*remaining_tasks, return_exceptions=True))
            self.console.log("Crawler finished.")


class AioRequest(AsyncRequestAdapter):
    async def execute(self, request: Request) -> Response:
        # Consider adding timeout configuration
        timeout = aiohttp.ClientTimeout(total=30) # Example timeout: 30 seconds total
        async with aiohttp.ClientSession(timeout=timeout) as session:
            self.parent_engine.console.log(f"Fetching {request.url} with depth {request.depth}") # Example of accessing parent for logging
            async with session.get(request.url, allow_redirects=True) as response: # allow_redirects is often useful
                body = await response.read()
                # Ensure headers are converted to a simple Dict[str, str] if needed
                # response.headers is a CIMultiDict
                headers = {str(k): str(v) for k, v in response.headers.items()}
                return Response(request=request, body=body, status_code=response.status, headers=headers)

    def __init__(self, engine_console = None, parent_engine=None): # Added for console logging in execute
        self.parent_engine = parent_engine


class SimpleRegexDiscoverer(DiscovererAdapter):
    # Improved regex to be a bit more specific, but still simple
    # This regex is basic and might find URLs in comments, JS strings, etc.
    # For robust parsing, an HTML parser (e.g. BeautifulSoup) is recommended.
    URL_REGEX = re.compile(r'href=["\'](https?://[^"\']+)["\']', re.IGNORECASE)

    def discover(self, response: Response) -> list[str]:
        try:
            html_content = response.body.decode(errors="ignore")
            # Find unique URLs to avoid processing duplicates from the same page
            urls = list(set(self.URL_REGEX.findall(html_content)))
            # For a more general case if not just hrefs:
            # urls = list(set(re.findall(r'https?://\S+', html_content)))
            return urls
        except Exception as e:
            # self.parent_engine.console.log(f"Error decoding or regexing response from {response.request.url}: {e}", style="red") # if parent_engine is available
            print(f"Error decoding or regexing response from {response.request.url}: {e}")
            return []

def parse_to_dict(response: Response) -> Dict:
    return {
        "url": response.request.url,
        "status_code": response.status_code,
        "headers": response.headers
    }

async def write_to_file(data: Dict):
    async with aiofiles.open("output.jsonl", "a") as f:
        await f.write(str(data) + "\n")
    return "finish"


if __name__ == "__main__":
    # Create the pipeline instance
    pipeline = Pipeline()
    
    pipeline.add_processor(input_type=Response, output_type=Dict, processor_callable=parse_to_dict)
    pipeline.add_processor(input_type=Dict, output_type=str, processor_callable=write_to_file)

    # Create the engine instance
    # We need to pass the engine instance (or its console) to adapters if they need to log
    # This creates a bit of a circular dependency or requires careful instantiation order.
    # A simpler way is to pass console directly if needed.
    
    engine = Engine(
        request_adapter=None, # Will be set below
        discoverer_adapter=SimpleRegexDiscoverer(),
        pipeline=pipeline,
        initial_max_depth=1, # Crawl start_url and links on it (depth 0 and 1)
        max_workers=5
    )
    
    # Now set the request adapter, passing the engine for logging (example)
    engine.request_adapter = AioRequest(parent_engine=engine)
    
    engine.run(start_url="https://blog.yurin.top", max_depth_override=2)
