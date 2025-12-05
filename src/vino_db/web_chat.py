import os

import tomllib
from playwright.async_api import async_playwright
from pydantic import BaseModel, Field


class ChatUIResponse(BaseModel):
    raw_text: str = Field(..., description="Raw response text from chat web UI")


class ChatWebUIClient:
    """
    A general client to interact with chat-like web UIs via Playwright.
    Supports initialization from a TOML config file or direct parameters.
    """

    def __init__(
        self,
        ui_url: str,
        input_selector: str,
        submit_selector: str,
        response_selector: str,
        headless: bool = True,
        timeout: int = 30000,
    ):
        """
        :param ui_url: URL where the chat UI is hosted
        :param input_selector: CSS selector or Playwright locator for input textbox
        :param submit_selector: CSS selector or Playwright locator for submit button
        :param response_selector: CSS selector or Playwright locator to extract response text
        :param headless: Whether to launch browser in headless mode
        :param timeout: Timeout for page navigation and selector waits (ms)
        """
        from urllib.parse import urlparse

        if not urlparse(ui_url).scheme:
            raise ValueError("Invalid URL provided")
        if not all([input_selector, submit_selector, response_selector]):
            raise ValueError("Selectors cannot be empty")
        self.ui_url = ui_url
        self.input_selector = input_selector
        self.submit_selector = submit_selector
        self.response_selector = response_selector
        self.headless = headless
        self.timeout = timeout

    @classmethod
    def from_config(cls, config_path: str, service_name: str) -> "ChatWebUIClient":
        """
        Initialize from a TOML config file for a specific service.

        :param config_path: Path to the TOML configuration file
        :param service_name: Name of the service (e.g., 'perplexity')
        :return: ChatWebUIClient instance
        """
        if not os.path.exists(config_path):
            raise FileNotFoundError(f"Config file not found: {config_path}")

        with open(config_path, "rb") as f:  # Open in binary mode for tomllib
            config = tomllib.load(f)
        service_config = config.get("services", {}).get(service_name)
        if not service_config:
            raise KeyError(f"Service '{service_name}' not found in config file")

        return cls(
            ui_url=service_config["ui_url"],
            input_selector=service_config["input_selector"],
            submit_selector=service_config["submit_selector"],
            response_selector=service_config["response_selector"],
            headless=service_config.get("headless", True),
            timeout=service_config.get("timeout", 30000),
        )

    async def run_prompt(self, prompt: str) -> ChatUIResponse:
        """
        Runs the prompt on the chat UI and returns the response.

        :param prompt: The prompt string to submit
        :return: ChatUIResponse object containing the raw text response
        """
        try:
            async with async_playwright() as pw:
                browser = await pw.chromium.launch(headless=self.headless)
                page = await browser.new_page()
                await page.goto(self.ui_url, timeout=self.timeout)
                await page.fill(self.input_selector, prompt)
                await page.click(self.submit_selector)
                await page.wait_for_selector(
                    self.response_selector, timeout=self.timeout
                )
                raw_response = await page.inner_text(self.response_selector)
                await browser.close()
                return ChatUIResponse(raw_text=raw_response)
        except Exception as e:
            raise RuntimeError(f"Failed to run prompt: {str(e)}")
