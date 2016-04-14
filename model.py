import collections
import itertools

from datetime import datetime
from ast import literal_eval


transaction_fields = [
    'id',
    'ts',
    'price',
    'amount',
    'sell',
    'asks',
    'bids',
    'd_high',
    'd_low',
    'd_vwap',
    'd_volume'
]

order_fields = ['price', 'amount']

TransactionBase = collections.namedtuple('Transaction', transaction_fields)
OrderBase = collections.namedtuple('Order', order_fields)


class Order(OrderBase):
    """
    OrderBase namedtuple overridden for type conversion on attributes
    """
    def __new__(cls, price, amount):
        return super(Order, cls).__new__(cls, price=float(price), amount=float(amount))


class Transaction(TransactionBase):
    """
    TransactionBase namedtuple overridden for type conversion on attributes
    """
    def __new__(cls, identifier, timestamp, price, amount, sell, asks, bids, d_high, d_low, d_vwap, d_volume):
        return super(Transaction, cls).__new__(
            cls,
            id=int(identifier),
            ts=datetime.strptime(timestamp, '%Y-%m-%d %H:%M:%S'),
            price=float(price),
            amount=float(amount),
            sell=sell == 't',
            asks=[Order(*ask) for ask in literal_eval(asks.replace('{', '(').replace('}', ')'))],
            bids=[Order(*bid) for bid in literal_eval(bids.replace('{', '(').replace('}', ')'))],
            d_high=float(d_high),
            d_low=float(d_low),
            d_vwap=float(d_vwap),
            d_volume=float(d_volume)
        )
