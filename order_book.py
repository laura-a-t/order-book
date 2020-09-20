import numpy as np

SELL_SIDE = 'S'


class OrderBook:
    def __init__(self):
        self.order_book = {}

    def get(self, key):
        return self.order_book.get(key, np.array([[]]))

    def update(self, symbol, side, size, price):
        key = self.get_key(symbol, side)
        if key in self.order_book:
            self.order_book[key], insert_index = self.insert(key, side, size, price)
        else:
            self.order_book[key] = np.array([[price], [size]])
            insert_index = 0
        return insert_index

    def insert(self, key, side, size, price):
        levels = self.order_book[key]
        index, level_exists = self.find_update_index(levels[0], price, ascending=side == SELL_SIDE)
        if level_exists:
            new_size = levels[1, index] + size
            if new_size:
                levels[1, index] = new_size
            else:
                levels = np.delete(levels, index, 1)
        else:
            levels = np.insert(levels, index, (price, size), axis=1)
        return levels, index

    @staticmethod
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

    @staticmethod
    def get_key(symbol, side):
        return f"{symbol}_{side}"
