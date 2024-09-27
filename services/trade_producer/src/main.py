from loguru import logger  # to create the log statements 
from quixstreams import Application
from src.kraken_websocket import KrakenWebsocketAPI
from websocket import create_connection
from typing import List
from src.trade import Trade

def produce_trades(
    kafka_broker_address: str,
    kafka_topic: str,
    product_id: str,
):
    """
    Reads trades from the Kraken Websocket API and saves them in the given `kafka_topic`

    Args:
        kafka_broker_address (str): The address of the Kafka broker
        kafka_topic (str): The Kafka topic to save the trades
        product_id (str): The product ID to get the trades from

    Returns:
        None
    """ 

    app = Application(
        broker_address=kafka_broker_address
    )

    # Define a topic "my_topic" with JSON serialization
    topic = app.topic(name=kafka_topic, value_serializer='json')

    # create a kraken api object
    kraken_api = KrakenWebsocketAPI(
        product_id=product_id
    )

    # Create a Producer instance
    with app.get_producer() as producer:

        while True:
            
            trades: List[Trade] = kraken_api.get_trades()

            for trade in trades:
                # Serialize an event using the defined Topic
                # convert it in to a sequence of bytes
                # key helps Kafka understand which partitions to insert this data
                # since Kafka topics are like tables and same key goes to same table
                # this way you will be able to read and insert in parallel to different partitions 
                # value is the content of the input goes into Kafka
                message = topic.serialize(key=trade.product_id, value=trade.model_dump())

                # Produce a message into the Kafka topic
                producer.produce(
                    topic=topic.name, value=message.value, key=message.key
                )

                logger.debug(f"Pushed trade to Kafka: {trade}")


if __name__ == '__main__': # it means, if this script is the entry point, run below
    # below is the section that runs when executing this file on command line
    # can use pydantic settings to write your own config ettings

    from src.config import config

    produce_trades(
        kafka_broker_address=config.kafka_broker_address,
        kafka_topic=config.kafka_topic,
        product_id=config.product_id,
    )