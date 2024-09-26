from typing import List
from websocket import create_connection
from loguru import logger
import json
from pydantic import BaseModel
from src.trade import Trade

class KrakenWebsocketAPI:
    """
    Class to read real-time trades from Kraken
    """

    URL = 'wss://ws.kraken.com/v2'

    def __init__(self, product_id: str):
        """
        Initialize the KrakenWebsocketAPI instance

        Args:
            product_id (str): product_id from which we will get the trade info
        """
        self.product_id = product_id

        # establish connection to the Kraken websocket API
        self._ws = create_connection(self.URL)
        logger.debug('Connection established')

        # subscribe to the trades for the given `product_id`
        self._subscribe(product_id)

    def get_trades(self) -> List[Trade]:
        """
        Returns the latest batch of trades
        Args:
            None
        Returns:
            List[Trade]: a list of python objects
        """
        message = self._ws.recv()

        if 'heartbeat' in message:
            # when I get a heartbeat, I return an empty list
            logger.debug("Heartbeat received.")
            return []

        # parse the message string as a dictionary
        message = json.loads(message)

        # extract trades from the message['data'] field
        trades = []
        for trade in message['data']:
            # transform the timestamp from Kraken which is a string
            # like '2024-06-17T09:45:38.494012Z' into Unix
            # milliseconds

            trades.append(
                Trade(
                    product_id=trade['symbol'],
                    price=trade['price'],
                    quantity=trade['qty'],
                    timestamp_ms=self.to_ms(trade['timestamp']),
                )
            )


        return trades

    def is_done(self) -> bool:
        """
        Tells whether we are done fetching data
        """
        False

    

    def _subscribe(self, product_id: str):
        # _ is not supposed to be called externally, end user shouldn't worry about this
        """
        Establish connection to the Kraken websocket API and subscribe to the trades for the given `product_id`.
        """
        logger.info(f'Subscribing to trades for {product_id}')
        # let's subscribe to the trades for the given `product_id`
        # example from https://docs.kraken.com/api/docs/websocket-v2/trade
        msg = {
            'method': 'subscribe',
            'params': {
                'channel': 'trade',
                'symbol': [product_id],
                'snapshot': False,
            },
        }
        
        # below line subscribes to ws api for that given currency
        self._ws.send(json.dumps(msg))
        logger.info('Subscription worked!')

        # For each product_id we dump
        # the first 2 messages we got from the websocket, because they contain
        # no trade data, just confirmation on their end that the subscription was successful
        for product_id in [product_id]:
            _ = self._ws.recv()
            _ = self._ws.recv()


    @staticmethod
    def to_ms(timestamp: str) -> int:
        """
        A function that transforms a timestamps expressed
        as a string like this '2024-06-17T09:36:39.467866Z'
        into a timestamp expressed in milliseconds.

        Args:
            timestamp (str): A timestamp expressed as a string.

        Returns:
            int: A timestamp expressed in milliseconds.
        """
        # parse a string like this '2024-06-17T09:36:39.467866Z'
        # into a datetime object assuming UTC timezone
        # and then transform this datetime object into Unix timestamp
        # expressed in milliseconds
        from datetime import datetime, timezone

        timestamp = datetime.fromisoformat(timestamp[:-1]).replace(tzinfo=timezone.utc)
        return int(timestamp.timestamp() * 1000)
