"""
Central place for runtime settings.

Values come from environment variables or a `.env`
file in the project root.  (python-dotenv is already
in your requirements.)
"""

from pydantic_settings import BaseSettings
from pydantic import Field
from typing import Optional


class Settings(BaseSettings):
    # --- GitHub ---
    github_token: Optional[str] = Field(
        default="", env="GITHUB_TOKEN",
        description="Personal-access token to lift the rate-limit to 5 000 req/h",
    )

    # --- Collector loop ---
    poll_interval: int = Field(
        default=30, env="POLL_INTERVAL",
        description="Seconds between hits to https://api.github.com/events",
        ge=5, le=300,
    )

    max_minutes: int = Field(
        default=1440, env="MAX_MINUTES",
        description="How many minutes of history to keep in EventStore",
        ge=10, le=60 * 24 * 7,        # 1 week max
    )

    class Config:  # noqa: D106
        env_file = ".env"
        env_file_encoding = "utf-8"
        case_sensitive = False

    dynamo_table: str = Field("GitHubEvents", env="GitHubEvents",
                              description="Name of the GitHubEvents DynamoDB table")
    aws_region: str = Field("eu-central-1", env="AWS_REGION",
                            description="AWS region for DynamoDB")

settings = Settings()
