import csv

import numpy as np
from scipy.io import savemat

from model import Transaction


def deserializer(csv_iterable):
    """
    Generator for converting csv to python
    Handle sparse data and yield
    Transaction model field order (same as csv)
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
    """
    previous_transaction = None

    while True:
        identifier, ts, price, amount, sell, asks, bids, d_high, d_low, d_vwap, d_volume = next(csv_iterable)

        if not d_high or not d_low or not d_vwap or not d_volume:
            # Sparse data, set current daily values to previous one
            # Since it does not change that much
            d_high = previous_transaction.d_high
            d_low = previous_transaction.d_low
            d_vwap = previous_transaction.d_vwap
            d_volume = previous_transaction.d_volume

        if not ts or not sell:
            # Cannot do anything for this, passing
            yield None
        else:
            transaction = Transaction(identifier, ts, price, amount, sell, asks, bids, d_high, d_low, d_vwap, d_volume)

            yield transaction
            previous_transaction = transaction


def serialize(transactions, output_filename):
    """
    Convert python to .mat
    """
    x = list()
    y = list()

    for transaction in transactions:
        x.append(np.array([
            transaction.amount,
            transaction.sell,
            transaction.asks[0][0],
            transaction.asks[0][1],
            transaction.asks[1][0],
            transaction.asks[1][1],
            transaction.asks[2][0],
            transaction.asks[2][1],
            transaction.asks[3][0],
            transaction.asks[3][1],
            transaction.asks[4][0],
            transaction.asks[4][1],
            transaction.asks[5][0],
            transaction.asks[5][1],
            transaction.asks[6][0],
            transaction.asks[6][1],
            transaction.asks[7][0],
            transaction.asks[7][1],
            transaction.asks[8][0],
            transaction.asks[8][1],
            transaction.asks[9][0],
            transaction.asks[9][1],
            transaction.bids[0][0],
            transaction.bids[0][1],
            transaction.bids[1][0],
            transaction.bids[1][1],
            transaction.bids[2][0],
            transaction.bids[2][1],
            transaction.bids[3][0],
            transaction.bids[3][1],
            transaction.bids[4][0],
            transaction.bids[4][1],
            transaction.bids[5][0],
            transaction.bids[5][1],
            transaction.bids[6][0],
            transaction.bids[6][1],
            transaction.bids[7][0],
            transaction.bids[7][1],
            transaction.bids[8][0],
            transaction.bids[8][1],
            transaction.bids[9][0],
            transaction.bids[9][1],
            transaction.d_high,
            transaction.d_low,
            transaction.d_vwap,
            transaction.d_volume
        ]))

        y.append(transaction.price)

    savemat(output_filename, dict(x=np.array(x), y=np.array(y)))


def process(input_filename, output_filename):
    """
    Process csv file and write two separate X and Y .mat files
    """
    transactions = []

    with open(input_filename, 'rb') as f:
        file_has_header = csv.Sniffer().has_header(f.read(1024))
        f.seek(0)
        csv_iterator = csv.reader(f)

        if file_has_header:
            next(csv_iterator)  # skip csv header bs

        csv_iterator = deserializer(csv_iterator)

        for transaction in csv_iterator:
            if transaction:
                transactions.append(transaction)

    return serialize(sorted(transactions, key=lambda tran: tran.id), output_filename)


