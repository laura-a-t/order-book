import os


class Logger:
    def __init__(self, log_path='output.log'):
        self.log_path = log_path
        # This makes sure the logger starts with an empty file
        with open(self.log_path, "w"):
            pass

    def print_log(self, sequence_no, symbol, order_book_bid, order_book_ask, order_book_depth):
        order_book_bid = order_book_bid[:, :order_book_depth]
        order_book_ask = order_book_ask[:, :order_book_depth]
        log = open(self.log_path, "a")
        log.write(f"{sequence_no}, "
                  f"{symbol}, "
                  f"{self._format_levels_for_printing(order_book_bid)}, "
                  f"{self._format_levels_for_printing(order_book_ask)}"
                  f"{os.linesep}")
        log.close()

    def _format_levels_for_printing(self, levels):
        if levels.size == 0:
            return '[]'
        return "[" + ', '.join(self._to_str(levels[:, i]) for i in range(levels.shape[1])) + "]"

    @staticmethod
    def _to_str(level):
        return "(" + ', '.join(str(i) for i in level) + ")"
