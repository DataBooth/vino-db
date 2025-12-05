import click
import tomllib
import asyncio
from pathlib import Path

from vino_db.web_chat import ChatWebUIClient

CONFIG_PATH = "conf/config.toml"


def get_available_services(config_path: str) -> tuple[list[str], str]:
    """Load available service names and default service from the TOML config."""
    if not Path(config_path).exists():
        raise FileNotFoundError(f"Config file not found: {config_path}")
    try:
        with open(config_path, "rb") as f:  # Open in binary mode for tomllib
            config = tomllib.load(f)
        services = list(config.get("services", {}).keys())
        default_service = config.get("default_service", "")
        if not default_service and services:
            default_service = services[
                0
            ]  # Fallback to first service if default_service is not set
        return services, default_service
    except tomllib.TOMLDecodeError as e:
        raise ValueError(f"Invalid TOML file: {e}")


@click.group()
def cli():
    """CLI for interacting with chat web UIs."""
    pass


@cli.command()
@click.option("--config", default=CONFIG_PATH, help="Path to TOML config file")
def list_services(config: str):
    """List available chat services from the config file."""
    try:
        services, default_service = get_available_services(config)
        if not services:
            click.echo("No services found in config file.")
            return
        click.echo(f"Available chat services (default: {default_service}):")
        for service in services:
            click.echo(f"- {service}")
    except FileNotFoundError as e:
        click.echo(f"Error: {e}")
    except ValueError as e:
        click.echo(f"Error: {e}")
    except Exception as e:
        click.echo(f"Error loading services: {e}")


@cli.command()
@click.option(
    "--service",
    default=None,
    help="Name of the chat service (e.g., perplexity). Defaults to config's default_service.",
)
@click.option("--prompt", default=None, help="Prompt to send to the chat UI")
@click.option(
    "--prompt-file",
    default=None,
    type=click.Path(exists=True, dir_okay=False),
    help="Path to a .md file containing the prompt",
)
@click.option("--config", default=CONFIG_PATH, help="Path to TOML config file")
def run_prompt(service: str, prompt: str, prompt_file: str, config: str):
    """Run a prompt on the selected chat service."""
    try:
        # Load services and default service
        services, default_service = get_available_services(config)
        if not services:
            raise click.UsageError("No services defined in config file")

        # Use default service if none provided
        selected_service = service if service else default_service
        if not selected_service:
            raise click.UsageError(
                "No default service defined in config and no service specified"
            )

        # Validate selected service
        if selected_service not in services:
            raise click.UsageError(
                f"Service '{selected_service}' not found. Available: {', '.join(services)}"
            )

        # Ensure exactly one of --prompt or --prompt-file is provided
        if (prompt is None and prompt_file is None) or (
            prompt is not None and prompt_file is not None
        ):
            raise click.UsageError(
                "Must provide either --prompt or --prompt-file, but not both"
            )

        # If prompt-file is provided, read the file
        if prompt_file:
            if not prompt_file.endswith(".md"):
                raise click.UsageError("Prompt file must have a .md extension")
            with open(prompt_file, "r", encoding="utf-8") as f:
                prompt = f.read().strip()

        # Ensure prompt is not empty
        if not prompt:
            raise click.UsageError("Prompt cannot be empty")

        client = ChatWebUIClient.from_config(config, selected_service)
        response = asyncio.run(client.run_prompt(prompt))
        click.echo(f"Response from {selected_service}:\n{response.raw_text}")
    except FileNotFoundError as e:
        click.echo(f"Error: {e}")
    except KeyError as e:
        click.echo(f"Error: {e}")
    except RuntimeError as e:
        click.echo(f"Error running prompt: {e}")
    except click.UsageError as e:
        click.echo(f"Error: {e}")
    except Exception as e:
        click.echo(f"Unexpected error: {e}")


if __name__ == "__main__":
    cli()
