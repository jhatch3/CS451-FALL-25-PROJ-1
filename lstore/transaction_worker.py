from lstore.table import Table, Record
from lstore.index import Index
import threading

class TransactionWorker:

    """
    # Creates a transaction worker object.
    """
    def __init__(self, transactions=None):
        self.stats = []
        self.transactions = list(transactions) if transactions else []
        self.result = 0
        self.thread = None

    
    """
    Appends t to transactions
    """
    def add_transaction(self, t):
        self.transactions.append(t)

        
    """
    Runs all transaction as a thread
    """
    def run(self):
        # launch worker thread
        self.thread = threading.Thread(target=self.__run)
        self.thread.start()


    """
    Waits for the worker to finish
    """
    def join(self):
        if self.thread is not None:
            self.thread.join()


    def __run(self):
        for transaction in self.transactions:
            # retry aborted transaction until commit
            ok = transaction.run()
            while ok is False:
                ok = transaction.run()
            self.stats.append(True)
        self.result = sum(1 for x in self.stats if x)

