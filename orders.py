

class Orders:
    def __init__(self):
        self.orders = {}

    def insert(self, key, order):
        self.orders[key] = order

    def get(self, key):
        return self.orders[key]

    def delete(self, key):
        del self.orders[key]

    def reduce_order_size(self, key, executed_size):
        order = self.get(key)
        new_size = order["size"] - executed_size
        if new_size:
            order['size'] = new_size
            self.insert(key, order)
        else:
            self.delete(key)

    @staticmethod
    def get_key(order_id, side):
        return f"{order_id}_{side}"
