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


def parse_messages(path):
    file = open(path, "rb")
    while True:
        sequence_no_bytes = file.read(4)
        if not sequence_no_bytes:
            break
        sequence_no = int.from_bytes(sequence_no_bytes, LITTLE_ENDIAN)
        # message_size
        file.read(4)
        message_type = file.read(1).decode(UTF_8_ENCODING)
        if message_type == MESSAGE_TYPE_ADDED:
            symbol = _process_order_added(file)
        elif message_type == MESSAGE_TYPE_UPDATED:
            symbol = _process_order_updated(file)
        elif message_type == MESSAGE_TYPE_DELETED:
            symbol = _process_order_deleted(file)
        elif message_type == MESSAGE_TYPE_EXECUTED:
            symbol = _process_order_executed(file)
        else:
            raise Exception("Incorrect message type and message size combination")
        key_bid = _get_key(symbol, BUY_SIDE)
        key_ask = _get_key(symbol, SELL_SIDE)
        print(f"{sequence_no}, "
              f"{symbol}, "
              f"{_format_levels_for_printing(order_book.get(key_bid))}, "
              f"{_format_levels_for_printing(order_book.get(key_ask))}")


def _format_levels_for_printing(levels):
    if levels is None:
        return '[]'
    _len = levels.shape[1]
    return "[" + ', '.join(_to_str(levels[:, i]) for i in range(levels.shape[1])) + "]"


def _to_str(l):
    return "(" + ', '.join(str(i) for i in l) + ")"


def _process_order_executed(file):
    message = _extract_message_executed(file)
    price = orders[message[1]][4]
    update_executed_order(message)
    update_order_book(message[0], message[2], -message[3], price)
    return message[0]


def _process_order_deleted(file):
    message = _extract_message_deleted(file)
    order = orders[message[1]]
    del orders[message[1]]
    update_order_book(order[0], order[2], -order[3], order[4])
    return message[0]


def _process_order_updated(file):
    message = _extract_message_added_or_updated(file)
    prev_order = orders[message[1]]
    update_order_book(message[0], message[2], -prev_order[3], prev_order[4])
    update_order_book(message[0], message[2], message[3], message[4])
    orders[message[1]] = message
    return message[0]


def _process_order_added(file):
    message = _extract_message_added_or_updated(file)
    orders[message[1]] = message
    update_order_book(message[0], message[2], message[3], message[4])
    return message[0]


def update_order_book(symbol, side, size, price):
    key = _get_key(symbol, side)
    if key in order_book:
        order_book[key] = _insert_order(order_book[key], size, price)
    else:
        order_book[key] = np.array([[price], [size]])


def _get_key(symbol, side):
    return f"{symbol}_{side}"


def _insert_order(levels, size, price):
    index = np.searchsorted(levels[0], price)
    if price in levels[0]:
        new_size = levels[1, index] + size
        if new_size:
            levels[1, index] = new_size
        else:
            levels = np.delete(levels, index, 1)
    else:
        levels = np.insert(levels, index, (price, size), axis=1)
    return levels


def update_executed_order(executed):
    order = orders[executed[1]]
    new_size = order[3] - executed[3]
    if new_size:
        orders[executed[1]] = order[0], order[1], order[2], new_size, order[4]
    else:
        del orders[executed[1]]


def _extract_message_added_or_updated(file):
    symbol = _extract_symbol(file)
    order_id = _extract_order_id(file)
    side = _extract_side(file)
    file.read(3)
    size = _extract_size(file)
    price = _extract_price(file)
    file.read(4)
    return symbol, order_id, side, size, price


def _extract_message_deleted(file):
    symbol = _extract_symbol(file)
    order_id = _extract_order_id(file)
    side = _extract_side(file)
    file.read(3)
    return symbol, order_id, side


def _extract_message_executed(file):
    symbol = _extract_symbol(file)
    order_id = _extract_order_id(file)
    side = _extract_side(file)
    file.read(3)
    traded_qty = _extract_size(file)
    return symbol, order_id, side, traded_qty


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


parse_messages('input1.stream')
