from typing import Optional

from pydantic_settings import BaseSettings

class AppConfig(BaseSettings):

    kafka_broker_address: Optional[str] = None
    kafka_input_topic: str
    kafka_output_topic: str
    kafka_consumer_group: str
    ohlcv_window_seconds: int
   
    # this is the first time I use this construct to load the environment variables from
    # an .env file
    class Config:
        env_file = ".env"

config = AppConfig()