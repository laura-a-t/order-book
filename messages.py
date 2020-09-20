
LITTLE_ENDIAN = 'little'
MESSAGE_TYPE_ADDED = "A"
MESSAGE_TYPE_UPDATED = "U"
MESSAGE_TYPE_DELETED = "D"
MESSAGE_TYPE_EXECUTED = "E"
UTF_8_ENCODING = "utf-8"


class Processor:
    def __init__(self, orders, order_book):
        self.processors = {
            MESSAGE_TYPE_ADDED: self._process_add_message,
            MESSAGE_TYPE_UPDATED: self._process_update_message,
            MESSAGE_TYPE_DELETED: self._process_delete_message,
            MESSAGE_TYPE_EXECUTED: self._process_execute_message
        }
        self.orders = orders
        self.order_book = order_book

    def process_message(self, message):
        process_func = self.processors[message["type"]]
        return process_func(message["message"])

    def _process_add_message(self, message):
        orders_key = self.orders.get_key(message["order_id"], message["side"])
        self.orders.insert(orders_key, message)

        return self.order_book.update(message["symbol"], message["side"], message["size"], message["price"])

    def _process_update_message(self, message):
        orders_key = self.orders.get_key(message["order_id"], message["side"])
        prev_order = self.orders.get(orders_key)
        self.orders.insert(orders_key, message)

        level_updated1 = self.order_book.update(
            prev_order["symbol"],
            prev_order["side"],
            -prev_order["size"],
            prev_order["price"])
        level_updated2 = self.order_book.update(message["symbol"], message["side"], message["size"], message["price"])
        return min(level_updated1, level_updated2)

    def _process_delete_message(self, message):
        orders_key = self.orders.get_key(message["order_id"], message["side"])
        order = self.orders.get(orders_key)
        self.orders.delete(orders_key)

        return self.order_book.update(message["symbol"], message["side"], -order["size"], order["price"])

    def _process_execute_message(self, message):
        orders_key = self.orders.get_key(message["order_id"], message["side"])
        price = self.orders.get(orders_key)["price"]
        self.orders.reduce_order_size(orders_key, message["size"])

        return self.order_book.update(message["symbol"], message["side"], -message["size"], price)


class Parser:
    def __init__(self):
        self.parsers = {
            MESSAGE_TYPE_ADDED: self._parse_add_message,
            MESSAGE_TYPE_UPDATED: self._parse_update_message,
            MESSAGE_TYPE_DELETED: self._parse_delete_message,
            MESSAGE_TYPE_EXECUTED: self._parse_execute_message
        }

    def parse_message(self, _bytes):
        message_type = _bytes.read(1).decode(UTF_8_ENCODING)
        parse_func = self.parsers[message_type]
        message = parse_func(_bytes)
        _bytes.close()
        return {
            "type": message_type,
            "message": message
        }

    def _parse_add_message(self, _bytes):
        symbol = self._extract_symbol(_bytes)
        order_id = self._extract_order_id(_bytes)
        side = self._extract_side(_bytes)
        _bytes.read(3)
        size = self._extract_size(_bytes)
        price = self._extract_price(_bytes)
        return {
            "symbol": symbol,
            "order_id": order_id,
            "side": side,
            "size": size,
            "price": price,
        }

    def _parse_update_message(self, _bytes):
        return self._parse_add_message(_bytes)

    def _parse_delete_message(self, _bytes):
        symbol = self._extract_symbol(_bytes)
        order_id = self._extract_order_id(_bytes)
        side = self._extract_side(_bytes)
        return {
            "symbol": symbol,
            "order_id": order_id,
            "side": side,
        }

    def _parse_execute_message(self, _bytes):
        symbol = self._extract_symbol(_bytes)
        order_id = self._extract_order_id(_bytes)
        side = self._extract_side(_bytes)
        _bytes.read(3)
        traded_qty = self._extract_size(_bytes)
        return {
            "symbol": symbol,
            "order_id": order_id,
            "side": side,
            "size": traded_qty,
        }

    @staticmethod
    def _extract_symbol(_bytes):
        return _bytes.read(3).decode(UTF_8_ENCODING)

    @staticmethod
    def _extract_order_id(_bytes):
        return int.from_bytes(_bytes.read(8), LITTLE_ENDIAN)

    @staticmethod
    def _extract_side(_bytes):
        return _bytes.read(1).decode(UTF_8_ENCODING)

    @staticmethod
    def _extract_size(_bytes):
        return int.from_bytes(_bytes.read(8), LITTLE_ENDIAN)

    @staticmethod
    def _extract_price(_bytes):
        return int.from_bytes(_bytes.read(4), LITTLE_ENDIAN, signed=True)
