#!/usr/bin/env python

import importlib

import typer

from yacrawler.utilities.aioadapter import AioRequest

cli = typer.Typer(help="CLI for running apps and crawling websites.")

app_path_argument = typer.Argument(..., help="Path to the app, e.g. 'myproject.app' or 'myproject.module:app'")

start_url_argument = typer.Argument(..., help="The starting URL for the crawler.")
max_depth_option = typer.Option(1, "--max-depth", "-d", help="Maximum depth to crawl.")
max_workers_option = typer.Option(5, "--max-workers", "-w", help="Maximum number of concurrent workers.")


@cli.command(help="Run a user-defined app, e.g. 'myproject.app' or 'myproject.module:app'.")
def run(app_path: str = app_path_argument):
    """
    Run a user-defined app specified by a Python path. Examples:

    - myproject.app
    - myproject.module:app
    """
    module_name: str = ""
    attr: str = ""
    try:
        if ":" in app_path:
            module_name, attr = app_path.split(":", 1)
        else:
            # Handle cases like 'myproject.app' where 'app' is an attribute of 'myproject'
            # or 'myproject.module.app_object' where 'app_object' is an attribute of 'myproject.module'.
            parts = app_path.rsplit(".", 1)
            if len(parts) == 2:
                module_name, attr = parts
            else:
                # Assume the app_path is a module name, and the app object is named 'app' within it.
                module_name = app_path
                attr = "app"  # Default attribute name if not specified

        # Import the module
        module = importlib.import_module(module_name)
        # Get the application object (attribute) from the module
        app = getattr(module, attr)

        # Check if the app object has a callable 'run' method
        if hasattr(app, 'run') and callable(app.run):
            app.run()
        else:
            typer.echo(f"❌ App object '{attr}' in module '{module_name}' does not have a callable 'run' method.",
                       err=True)
            raise typer.Exit(code=1)

    except ModuleNotFoundError:
        # If module_name was not successfully determined (e.g. empty string from bad split)
        # or if import_module fails.
        err_module_name = module_name if module_name else app_path.split(":")[0] if ":" in app_path else app_path
        typer.echo(f"❌ Module '{err_module_name}' not found.", err=True)
        raise typer.Exit(code=1)
    except AttributeError:
        typer.echo(f"❌ Attribute '{attr}' not found in module '{module_name}'.", err=True)
        raise typer.Exit(code=1)
    except Exception as e:
        typer.echo(f"❌ An unexpected error occurred while running app from '{app_path}': {e}", err=True)
        raise typer.Exit(code=1)


@cli.command(help="Start a crawler with the specified URL, depth, and number of workers.")
def crawl(start_url: str = start_url_argument, max_depth: int = max_depth_option,
        max_workers: int = max_workers_option):
    """
    Start a web crawler from the given URL with specified depth and concurrency level.
    """
    try:
        # Import yacrawler components only when the crawl command is called
        # This makes the 'crawl' command optional if yacrawler is not installed,
        # allowing the 'run' command to still function.
        from yacrawler.cli import CrawlerCliApp
        from yacrawler.core import AsyncRequestAdapter, Pipeline
        from yacrawler.utilities.discoverers import SimpleRegexDiscoverer

        typer.echo(f"Starting crawler from {start_url} with max depth {max_depth} and {max_workers} workers...")

        # Initialize and run the crawler application
        CrawlerCliApp(start_urls=[start_url], max_depth=max_depth, max_workers=max_workers,
            request_adapter=AioRequest(), discoverer_adapter=SimpleRegexDiscoverer(), pipeline=Pipeline()).run()

        typer.echo("Crawler finished.")

    except ImportError:
        typer.echo(
            "❌ yacrawler library not found. Please install it (`pip install yacrawler`) to use the crawl command.",
            err=True)
        raise typer.Exit(code=1)
    except Exception as e:
        typer.echo(f"❌ An error occurred during crawling: {e}", err=True)
        raise typer.Exit(code=1)


if __name__ == "__main__":
    # This block allows the script to be run directly (e.g., python run.py run ...)
    # When the package is installed and run as a module (e.g., python -m yacrawler ...),
    # the entry point configured in pyproject.toml (often pointing to this cli object) is used.
    cli()
