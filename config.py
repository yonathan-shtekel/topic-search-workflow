import os

from pydantic_settings import BaseSettings


class Env(BaseSettings):
    bootstrap_servers: str
    schema_registry_url: str
    sasl_username: str
    sasl_password: str
    redis_host: str
    redis_port: int
    search_timeout_minutes: int
    use_high_watermark_validation: bool = False

    class Config:
        env_file = ".env_workflow"


settings = Env()

os.environ["SCHEMA_REGISTRY_URL"] = settings.schema_registry_url
