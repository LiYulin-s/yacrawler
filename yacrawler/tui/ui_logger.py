from datetime import datetime
from typing import Optional

from textual.message import Message
from textual.widgets import RichLog, Tree

from yacrawler.core import LoggerAdapter


# from yacrawler.tui import CrawlerApp


class UpdateTreeNodeMessage(Message):
    """Message to update a node in the Textual Tree."""

    def __init__(self, url: str, label: str, parent_url: Optional[str] = None):
        self.url = url
        self.label = label
        self.parent_url = parent_url
        super().__init__()


class UILogger(LoggerAdapter):
    def __init__(self, app: "CrawlerApp"):
        self.app = app
        self.console_logger = self.app.query_one(RichLog)
        self.tree_logger = self.app.query_one(Tree)

    def log(self, message: str, level: str):
        timestamp = datetime.now().strftime("[%X]")
        match level:
            case "INFO":
                style_str = "white"
            case "WARNING":
                style_str = "yellow"
            case "ERROR":
                style_str = "red"
            case "CRITICAL":
                style_str = "red bold"
            case _:
                style_str = "white"
        self.console_logger.write(f"[{style_str}]{timestamp} {message} [/]")

    def update_node(self, url: str, label: str, status: str, parent_url: str):
        match status:
            case "PENDING":
                style_str = "white"
            case "VISITING":
                style_str = "yellow"
            case "VISITED":
                style_str = "green"
            case "PROCESSING":
                style_str = "blue"
            case "PROCESSED":
                style_str = "cyan"
            case "ERROR":
                style_str = "red"
            case "SKIPPED":
                style_str = "dim"
            case _:
                style_str = "white"

        styled_label = f"[{style_str}]{label} - {status}[/]"
        message = UpdateTreeNodeMessage(url, styled_label, parent_url)
        self.app.post_message(message)
