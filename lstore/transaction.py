from lstore.table import Table, Record
from lstore.index import Index
import threading

# Thread-safe transaction ID generator
_txn_id_counter = 0
_txn_id_lock = threading.Lock()

def _next_txn_id() -> int:
    """Generate a unique transaction ID (threadsafe"""
    global _txn_id_counter
    with _txn_id_lock:
        _txn_id_counter += 1
        return _txn_id_counter

class Transaction:

    """
    # Creates a transaction object.
    """
    def __init__(self, lock_manager=None):
        self.queries = []
        self.txn_id = _next_txn_id()  # unique ID for this transaction
        self.lock_manager = lock_manager  # reference to lock manager (set by caller if needed)
        self.active = True

    """
    # Adds the given query to this transaction
    # Example:
    # q = Query(grades_table)
    # t = Transaction()
    # t.add_query(q.update, grades_table, 0, *[None, 1, None, 2, None])
    """
    def add_query(self, query, table, *args):
        self.queries.append((query, args))
        # store table reference to get lock manager if needed
        if self.lock_manager is None and table is not None:
            self.lock_manager = getattr(table, 'lock_manager', None)

        
    # If you choose to implement this differently this method must still return True if transaction commits or False on abort
    def run(self):
        for query, args in self.queries:
            result = query(*args, txn_id = self.txn_id)
            # If the query has failed the transaction should abort
            if result == False:
                return self.abort()
        return self.commit()

    
    def abort(self):
        #TODO: do roll-back and any other necessary operations
        # release all locks held by this transaction
        if self.lock_manager is not None:
            self.lock_manager.release_all(self.txn_id)
        self.active = False
        #print("Transaction aborted.")
        return False

    
    def commit(self):
        # TODO: commit to database
        # release all locks held by this transaction (strict 2PL: release at commit)
        if self.lock_manager is not None:
            self.lock_manager.release_all(self.txn_id)
        self.active = False
       # print("Transaction committed.")
        return True


