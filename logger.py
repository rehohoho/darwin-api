import multiprocessing
import logging
from logging.handlers import QueueHandler, QueueListener
import time
import random


def test_f(i):
    time.sleep(random.uniform(.01, .05))
    logging.info('function called with {} in worker thread.'.format(i))
    time.sleep(random.uniform(.01, .05))
    return i


def worker_init(q):
    # all records from worker processes go to qh and then into q
    qh = QueueHandler(q)
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)
    logger.addHandler(qh)


def logger_init(path):
    q = multiprocessing.Queue()

    # this is the handler for all log records
    fhandler = logging.FileHandler(path, mode='w')
    fhandler.setLevel(logging.DEBUG)
    shandler = logging.StreamHandler()
    shandler.setLevel(logging.DEBUG)

    formatter = logging.Formatter("%(levelname)s: %(asctime)s - %(message)s")
    fhandler.setFormatter(formatter)
    shandler.setFormatter(formatter)

    # ql gets records from the queue and sends them to the handler
    ql = QueueListener(q, fhandler, shandler)
    ql.start()

    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)
    # add the handler to the logger so records from this process are handled
    logger.addHandler(fhandler)
    logger.addHandler(shandler)

    return ql, q
    

def single_thread_logger(path):
    file_handler = logging.FileHandler(path, mode="w")
    file_handler.setLevel(logging.INFO)
    stream_handler = logging.StreamHandler()
    stream_handler.setLevel(logging.INFO)
    
    formatter = logging.Formatter("%(levelname)s: %(asctime)s - %(message)s")
    stream_handler.setFormatter(formatter)
    file_handler.setFormatter(formatter)
    
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)
    logger.addHandler(stream_handler)
    logger.addHandler(file_handler)
    return logger


# TEST
if __name__ == '__main__':
    q_listener, q = logger_init('test.log')

    logging.info('hello from main thread')
    pool = multiprocessing.Pool(4, worker_init, [q])
    for result in pool.map(test_f, range(10)):
        pass
    pool.close()
    pool.join()
    q_listener.stop()