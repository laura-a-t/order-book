import time
import os

import numpy as np

LITTLE_ENDIAN = 'little'
MESSAGE_TYPE_ADDED = "A"
MESSAGE_TYPE_UPDATED = "U"
MESSAGE_TYPE_DELETED = "D"
MESSAGE_TYPE_EXECUTED = "E"
UTF_8_ENCODING = "utf-8"
BUY_SIDE = 'B'
SELL_SIDE = 'S'

order_book = {}
orders = {}


def parse_messages(path, order_book_depth):
    log = open('output.log', "w")
    file = open(path, "rb")
    while True:
        sequence_no_bytes = file.read(4)
        if not sequence_no_bytes:
            break
        sequence_no = int.from_bytes(sequence_no_bytes, LITTLE_ENDIAN)
        # The message size does not seem to be necessary except for asserting the message size is as expected per type
        # of message
        file.read(4)
        message_type = file.read(1).decode(UTF_8_ENCODING)
        if message_type == MESSAGE_TYPE_ADDED:
            symbol, level_updated = _process_order_added(file)
        elif message_type == MESSAGE_TYPE_UPDATED:
            symbol, level_updated = _process_order_updated(file)
        elif message_type == MESSAGE_TYPE_DELETED:
            symbol, level_updated = _process_order_deleted(file)
        elif message_type == MESSAGE_TYPE_EXECUTED:
            symbol, level_updated = _process_order_executed(file)
        else:
            raise Exception("Incorrect message type and message size combination")
        if level_updated < order_book_depth:
            _log_changes(log, sequence_no, symbol, order_book_depth)
    file.close()
    log.close()


def _log_changes(log, sequence_no, symbol, order_book_depth):
    key_bid = _get_order_book_key(symbol, BUY_SIDE)
    key_ask = _get_order_book_key(symbol, SELL_SIDE)
    order_book_bid = order_book.get(key_bid, np.array([[]]))[:, :order_book_depth]
    order_book_ask = order_book.get(key_ask, np.array([[]]))[:, :order_book_depth]
    log.write(f"{sequence_no}, "
              f"{symbol}, "
              f"{_format_levels_for_printing(order_book_bid)}, "
              f"{_format_levels_for_printing(order_book_ask)}"
              f"{os.linesep}")


def _format_levels_for_printing(levels):
    if levels.size == 0:
        return '[]'
    return "[" + ', '.join(_to_str(levels[:, i]) for i in range(levels.shape[1])) + "]"


def _to_str(l):
    return "(" + ', '.join(str(i) for i in l) + ")"


def _process_order_executed(file):
    message = _extract_message_executed(file)

    orders_key = _get_orders_key(message["order_id"], message["side"])
    price = get_order_from_list(orders_key)["price"]
    update_executed_order_in_list(orders_key, message)

    level_updated = update_order_book(message["symbol"], message["side"], -message["size"], price)
    return message["symbol"], level_updated


def _process_order_deleted(file):
    message = _extract_message_deleted(file)

    orders_key = _get_orders_key(message["order_id"], message["side"])
    order = get_order_from_list(orders_key)
    delete_order_from_list(orders_key)

    level_updated = update_order_book(message["symbol"], message["side"], -order["size"], order["price"])
    return message["symbol"], level_updated


def _process_order_updated(file):
    message = _extract_message_added_or_updated(file)

    orders_key = _get_orders_key(message["order_id"], message["side"])
    prev_order = get_order_from_list(orders_key)
    insert_update_order_in_list(orders_key, message)

    level_updated1 = update_order_book(prev_order["symbol"], prev_order["side"], -prev_order["size"], prev_order["price"])
    level_updated2 = update_order_book(message["symbol"], message["side"], message["size"], message["price"])
    return message["symbol"], min(level_updated1, level_updated2)


def _process_order_added(file):
    message = _extract_message_added_or_updated(file)

    orders_key = _get_orders_key(message["order_id"], message["side"])
    insert_update_order_in_list(orders_key, message)

    level_updated = update_order_book(message["symbol"], message["side"], message["size"], message["price"])
    return message["symbol"], level_updated


def insert_update_order_in_list(key, message):
    orders[key] = message


def get_order_from_list(key):
    return orders[key]


def delete_order_from_list(key):
    del orders[key]


def update_executed_order_in_list(key, message):
    order = get_order_from_list(key)
    new_size = order["size"] - message["size"]
    if new_size:
        order['size'] = new_size
        insert_update_order_in_list(key, order)
    else:
        delete_order_from_list(key)


def update_order_book(symbol, side, size, price):
    key = _get_order_book_key(symbol, side)
    if key in order_book:
        order_book[key], index = _insert_order_in_order_book(order_book[key], side, size, price)
    else:
        order_book[key] = np.array([[price], [size]])
        index = 0
    return index


def _get_order_book_key(symbol, side):
    return f"{symbol}_{side}"


def _get_orders_key(order_id, side):
    return f"{order_id}_{side}"


def _insert_order_in_order_book(levels, side, size, price):
    index, level_exists = find_update_index(levels[0], price, ascending=side == SELL_SIDE)
    if level_exists:
        new_size = levels[1, index] + size
        if new_size:
            levels[1, index] = new_size
        else:
            levels = np.delete(levels, index, 1)
    else:
        levels = np.insert(levels, index, (price, size), axis=1)
    return levels, index


def find_update_index(arr, value, ascending=True):
    low = 0
    high = len(arr)
    while low < high:
        mid = (low + high) // 2
        if arr[mid] == value:
            return mid, True
        if arr[mid] > value and ascending:
            high = mid
        elif arr[mid] < value and not ascending:
            high = mid
        else:
            low = mid + 1
    return low if ascending else high, False


def _extract_message_added_or_updated(file):
    symbol = _extract_symbol(file)
    order_id = _extract_order_id(file)
    side = _extract_side(file)
    file.read(3)
    size = _extract_size(file)
    price = _extract_price(file)
    file.read(4)
    return {
        "symbol": symbol,
        "order_id": order_id,
        "side": side,
        "size": size,
        "price": price,
    }


def _extract_message_deleted(file):
    symbol = _extract_symbol(file)
    order_id = _extract_order_id(file)
    side = _extract_side(file)
    file.read(3)
    return {
        "symbol": symbol,
        "order_id": order_id,
        "side": side,
    }


def _extract_message_executed(file):
    symbol = _extract_symbol(file)
    order_id = _extract_order_id(file)
    side = _extract_side(file)
    file.read(3)
    traded_qty = _extract_size(file)
    return {
        "symbol": symbol,
        "order_id": order_id,
        "side": side,
        "size": traded_qty,
    }


def _extract_symbol(file):
    return file.read(3).decode(UTF_8_ENCODING)


def _extract_order_id(file):
    return int.from_bytes(file.read(8), LITTLE_ENDIAN)


def _extract_side(file):
    return file.read(1).decode(UTF_8_ENCODING)


def _extract_size(file):
    return int.from_bytes(file.read(8), LITTLE_ENDIAN)


def _extract_price(file):
    return int.from_bytes(file.read(4), LITTLE_ENDIAN, signed=True)


t1 = time.time()
parse_messages('input2.stream', 10)
t2 = time.time()
print(f"Execution took {t2 - t1} seconds")
