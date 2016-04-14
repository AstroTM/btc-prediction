import csv

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


def serialize(transactions):
    """
    Convert python to .mat
    """
    # savemat('input.mat')
    return transactions


def process(file_name):
    """
    Process csv file and write two separate X and Y .mat files
    """
    transactions = []

    with open(file_name, 'rb') as f:
        file_has_header = csv.Sniffer().has_header(f.read(1024))
        f.seek(0)
        csv_iterator = csv.reader(f)

        if file_has_header:
            next(csv_iterator)  # skip csv header bs

        csv_iterator = deserializer(csv_iterator)

        for transaction in csv_iterator:
            if transaction:
                transactions.append(transaction)

    return serialize(transactions)



