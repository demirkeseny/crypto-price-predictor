from quixstreams import Application
from loguru import logger
import json
from src.hopsworks_api import push_value_to_feature_group
from typing import List


def topic_to_feature_store(
        kafka_broker_address:str,
        kafka_input_topic:str,
        kafka_consumer_group:str,
        feature_group_name:str,
        feature_group_version:int,
        feature_group_primary_keys: List[str],
        feature_group_event_time: str,

        # need some feature storage credentials
):
    """
    App that consumes data from Kafka
    Reads incoming messages from the given kafka_input_topic
    And pushes to given feature_group_name

    Args:
        kafka_broker_address (str): 
        kafka_input_topic (str): Kafka topic to read the messages from 
        kafka_consumer_group (str): 
        feature_group_name (str):
        feature_group_version (int):
        feature_group_primary_keys (List[str]):
        feature_group_event_time (str):

    Returns:
        None
    """


    # Configure an Application. 
    # The config params will be used for the Consumer instance too.
    app = Application(
        broker_address=kafka_broker_address, 
        #auto_offset_reset='earliest', # when spin up the consumer data, do you want to process all the data points or the latest?
                                    # or interested in the historical data as well?
        consumer_group=kafka_consumer_group,
    )

    # Create a consumer and start a polling loop
    with app.get_consumer() as consumer:
        consumer.subscribe(topics=[kafka_input_topic])

        while True:
            msg = consumer.poll(0.1) # how long are you waiting for kafka to say "is there a msg or not"
            
            if msg is None:
                continue
            elif msg.error():
                logger.error('Kafka error:', msg.error())
                continue

            value = msg.value()
            # decode the message bytes into a dict
            value = json.loads(value.decode('utf-8'))

            push_value_to_feature_group(
                value,
                feature_group_name,
                feature_group_version,
                feature_group_primary_keys,
                feature_group_event_time,
            )

            breakpoint()

            # push the value to feature store

            # Do some work with the value here ...

            # Store the offset of the processed message on the Consumer 
            # for the auto-commit mechanism.
            # It will send it to Kafka in the background.
            # Storing offset only after the message is processed enables at-least-once delivery
            # guarantees.
            consumer.store_offsets(message=msg)

if __name__ == "__main__":

    from src.config import config

    topic_to_feature_store(
        kafka_broker_address=config.kafka_broker_address,
        kafka_input_topic=config.kafka_input_topic,
        kafka_consumer_group=config.kafka_consumer_group,
        feature_group_name=config.feature_group_name,
        feature_group_version=config.feature_group_version,
        feature_group_primary_keys=config.feature_group_primary_keys,
        feature_group_event_time=config.feature_group_event_time,
        # start_offline_materialization=config.start_offline_materialization,
        # batch_size=config.batch_size,
    )