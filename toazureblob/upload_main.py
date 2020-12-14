from toazureblob.sourcetoblob import covid19,housing,stocks,unemployment
import logging
import os
import importlib
from queue import Queue
from threading import Thread
from time import time


logger = logging.getLogger('pyspark')
class UploadMain(Thread):

    def __init__(self, queue):
        Thread.__init__(self)
        self.queue = queue

    def run(self):
        while True:
            # Get the work from the queue and expand the tuple
            source = self.queue.get()
            try:
                source.upload_to_azure_blob()
            except Exception as ex:
                logger.error("Error executing upload_to_azure_blob %s",ex)
            finally:
                self.queue.task_done()



def main():
    ts = time()
    source_list = [stocks.StockMarketData(), unemployment.UnemploymentData(),housing.Housing(), covid19.Covid19()]
    queue = Queue()
    # Create 8 worker threads
    for x in range(8):
        worker = UploadMain(queue)
        # Setting daemon to True will let the main thread exit even though the workers are blocking
        worker.daemon = True
        worker.start()
    # Put the tasks into the queue as a tuple
    for source in source_list:
        logger.info('Queueing {}'.format(source))
        queue.put(source)
    # Causes the main thread to wait for the queue to finish processing all the tasks
    queue.join()
    logging.info('Took %s', time() - ts)

if __name__ == "__main__":
    filename = os.path.basename(__file__)
    module = os.path.splitext(filename)[0]
    module = importlib.import_module(module)
    module.main()

