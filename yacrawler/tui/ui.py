from typing import Dict, Optional, Any

from textual.app import App, ComposeResult
from textual.containers import VerticalScroll  # Use containers for layout
from textual.widgets import Header, Footer, Tree, RichLog  # Use RichLog for logs

from yacrawler.core import AsyncRequestAdapter, Pipeline, DiscovererAdapter, UrlWrapper
from yacrawler.tui.ui_logger import UILogger, UpdateTreeNodeMessage


class CrawlerApp(App[None]):
    """A Textual app to display the crawler progress."""

    CSS = """
    Screen {
        layout: grid;
        grid-size: 2;
        grid-columns: 1fr 2fr; /* Log on left (1 part), Tree on right (2 parts) */
        grid-rows: 1fr;
    }

    Header {
        column-span: 2; /* Header spans both columns */
    }

    #log-view {
        height: 100%; /* Fill the grid cell vertically */
        border: heavy blue;
        column-span: 1; /* Explicitly set column span */
        row-span: 1;    /* Explicitly set row span */
    }
     #log-view RichLog {
         /* Optional: Style the RichLog content */
         padding: 0 1; /* Add some horizontal padding */
     }


    #tree-view {
        height: 100%; /* Fill the grid cell vertically */
        border: heavy green;
        column-span: 1; /* Explicitly set column span */
        row-span: 1;    /* Explicitly set row span */
    }

    Footer {
        column-span: 2; /* Footer spans both columns */
    }
    """

    BINDINGS = [
        ("q", "quit", "Quit"),
        ("s", "stop", "Stop"),
    ]

    def __init__(self, start_url: str, max_depth: int, max_workers: int, request_adapter: AsyncRequestAdapter,
                 discoverer_adapter: DiscovererAdapter, pipeline: Pipeline):
        super().__init__()
        self.main_worker = None
        self.title = "Async Crawler Progress"
        self.start_url = start_url
        self.max_depth = max_depth
        self.max_workers = max_workers
        self.engine: Optional["Engine"] = None
        self.tree_nodes: Dict[str, Any] = {}  # Store Textual Tree Nodes

        self.request_adapter = request_adapter
        self.discoverer_adapter = discoverer_adapter
        self.pipeline = pipeline

    def compose(self) -> ComposeResult:
        """Compose our UI."""
        yield Header()
        # Use VerticalScroll and HorizontalScroll for panels within the grid cells
        with VerticalScroll(id="log-view"):
            yield RichLog(highlight=True, markup=True)  # Textual widget for logs
        with VerticalScroll(id="tree-view"):
            # Initialize the tree with a root label
            yield Tree("Crawler Progress")
        yield Footer()

    async def on_mount(self) -> None:
        """Called when the app is mounted."""
        from yacrawler.core import Engine
        self.engine = Engine(
            request_adapter=self.request_adapter,
            discoverer_adapter=self.discoverer_adapter,
            pipeline=self.pipeline,
            log_adapter=UILogger(self),
            initial_max_depth=self.max_depth,
            max_workers=self.max_workers
        )

        # Set engine reference in adapters
        self.request_adapter.set_engine(self.engine)
        self.discoverer_adapter.set_engine(self.engine)

        # Add the initial URL to the engine's queue
        initial_wrapper = UrlWrapper(self.start_url, 0, parent_url=None)
        self.engine.to_visit.append(initial_wrapper)

        # Get the tree widget and expand the root node on mount
        tree_widget = self.query_one(Tree)
        tree_widget.root.expand()

        # Run the crawler dispatch as a Textual worker
        # This integrates the async crawler logic with the Textual event loop
        self.main_worker = self.run_worker(self.engine.dispatch, exclusive=True)

    # Handler for the custom UpdateTreeNodeMessage
    def on_update_tree_node_message(self, message: UpdateTreeNodeMessage) -> None:
        """Handles updating a node in the tree based on a message."""
        tree_widget = self.query_one(Tree)
        url = message.url
        label = message.label
        parent_url = message.parent_url

        if url not in self.tree_nodes:
            # Node doesn't exist, create it
            if parent_url is None:
                # This is the root URL
                node = tree_widget.root.add(label)
                self.tree_nodes[url] = node
            else:
                # Find the parent node and add this URL as a child
                parent_node = self.tree_nodes.get(parent_url)
                if parent_node:
                    node = parent_node.add(label)
                    self.tree_nodes[url] = node
                    parent_node.expand()  # Expand parent to show new child
                else:
                    # Should not happen if logic is correct, but handle defensively
                    self.query_one(RichLog).write(
                        f"[yellow]Warning: Parent node not found for {url} (parent: {parent_url})[/]")
                    # Optionally add as a root child if parent not found
                    node = tree_widget.root.add(f"[yellow]Orphan: {label}[/]")
                    self.tree_nodes[url] = node
        else:
            # Node exists, update its label
            node = self.tree_nodes[url]
            node.set_label(label)  # Update the label with the new color/state

    def action_quit(self) -> None:
        """Called in response to key binding."""
        self.exit()  # Exit the Textual app

    def action_stop(self) -> None:
        """Called in response to key binding."""
        log = self.query_one(RichLog)
        log.write("[red]Stopping crawler...[/]")
        self.main_worker.cancel()  # Cancel the main worker
