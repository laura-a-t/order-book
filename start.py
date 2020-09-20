import time
from io import BytesIO

from logger import Logger
from messages import Parser, Processor
from order_book import OrderBook
from orders import Orders

LITTLE_ENDIAN = 'little'


def parse_messages(path, order_book_depth):
    logger = Logger()
    order_book = OrderBook()
    parser = Parser()
    processor = Processor(Orders(), order_book)
    file = open(path, "rb")
    while sequence_no_bytes := file.read(4):
        sequence_no = int.from_bytes(sequence_no_bytes, LITTLE_ENDIAN)
        message_size = int.from_bytes(file.read(4), LITTLE_ENDIAN)
        message = parser.parse_message(BytesIO(file.read(message_size)))
        level_updated = processor.process_message(message)

        if level_updated < order_book_depth:
            logger.print_log(sequence_no, message["message"]["symbol"], order_book, order_book_depth)
    file.close()


t1 = time.time()
parse_messages('input2.stream', 2)
t2 = time.time()
print(f"Execution took {t2 - t1} seconds")
