from quixstreams import Application
from loguru import logger
from datetime import timedelta


def init_ohlcv_candle(
        trade: dict
):
    """
    Returns the initial OHLCV candle when the first tarde in that w happens
    """
    return {
        'open':trade['price'],
        'high':trade['price'],
        'low':trade['price'],
        'close':trade['price'],
        'volume':trade['quantity'],
        # 'timestamp_ms':trade['timestamp_ms'],
    }

def update_ohlcv_candle(candle:dict,trade:dict):
    """
    Updates the OHLCV candle with the new trade
    """
    candle['high'] = max(candle['high'],trade['price'])
    candle['low'] = min(candle['low'],trade['price'])
    candle['close'] = trade['price']
    candle['volume'] += trade['quantity']

    return candle
    

def transform_trade_to_ohlcv(
    kafka_broker_address:str,
    kafka_input_topic:str,
    kafka_output_topic:str,
    kafka_consumer_group:str,
    ohlcv_window_seconds:int
 ):
    """
    Reads incoming trades from the given kafka_input_topc
    Transforms them into OHLC data
    Outputs them to the given kafka_output_topic

    Args:
        kafka_broker_address (str): 
        kafka_input_topic (str): The Kafka topic to read the trades from
        kafka_output_topic (str): The Kafka topic to save the OHLC data
        kafka_consumer_group (str): 
    Returns:
        None
    """

    app = Application(
            broker_address=kafka_broker_address,
            consumer_group=kafka_consumer_group,
    )
    input_topic = app.topic(name=kafka_input_topic,value_deserializer='json')
    output_topic = app.topic(name=kafka_output_topic,value_serializer='json')

    # Create a Quix Streams DataFrame
    sdf = app.dataframe(input_topic)
    # sdf.update(logger.debug) # reading incoming trade

    # main transformation into 1 min candles
    sdf = (
        sdf.tumbling_window(duration_ms=timedelta(seconds=ohlcv_window_seconds)) # time delta btw windows is 1sec
        .reduce(reducer=update_ohlcv_candle, initializer=init_ohlcv_candle) # how to apply the transformations to w
        .final() # just provide the final view after 1 sec w
    )

    # print the output to console
    # sdf.update(logger.debug)

    sdf['open'] = sdf['value']['open']
    sdf['high'] = sdf['value']['high']
    sdf['low'] = sdf['value']['low']
    sdf['close'] = sdf['value']['close']
    sdf['volume'] = sdf['value']['volume']
    sdf['timestamp_ms'] = sdf['end']

    # keep columns we are interested in
    sdf = sdf[['timestamp_ms','open','high','low','close','volume']]

    sdf.update(logger.debug)

    # push this message to output topic
    sdf = sdf.to_topic(output_topic)

    # kick off the app
    app.run(sdf)

# define python pre-emble
if __name__ == "__main__":

    transform_trade_to_ohlcv(
        kafka_broker_address='localhost:19092',
        kafka_input_topic='trade',
        kafka_output_topic='ohlcv',
        kafka_consumer_group='consumer_group_trade_to_ohlcv_2',
        ohlcv_window_seconds=60,
    )