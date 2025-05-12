from rich.console import Console

from yacrawler.core import LoggerAdapter


class CliLogger(LoggerAdapter):
    def __init__(self, console: Console):
        self.console_logger = console

    def log(self, message: str, level: str):
        match level:
            case "INFO":
                style_str = "cyan"
            case "WARNING":
                style_str = "yellow"
            case "ERROR":
                style_str = "red"
            case "CRITICAL":
                style_str = "red bold"
            case _:
                style_str = "white"
        self.console_logger.log(f"[{style_str}][{level}][/] {message}")

    def update_node(self, url: str, label: str, status: str, parent_url: str):
        pass
