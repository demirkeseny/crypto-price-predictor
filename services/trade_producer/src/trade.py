# use case for using pydantic
# this helps creating your own data structures

from pydantic import BaseModel


class Trade(BaseModel):
    """
    A class to represent a trade using Pydantic.
    """

    product_id: str
    quantity: float
    price: float
    timestamp_ms: int